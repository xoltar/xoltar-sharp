namespace Xoltar.Core.Transaction

open System.Collections.Generic
open System.Linq
open Microsoft.FSharp.Linq
open System.Transactions
open System.Threading

///<summary>A helper class for TransactionalDictionary</summary>
///TransactionValues is an IDictionary<,> that keeps track of what
///keys and values have been added and removed. Each TransactionalValues
///instance is associated with a single transaction, and when the transaction
///commits, this class will update the "backing store" that was initially
///passed to its constructor with the changes that have happened during the
///transaction.
type internal TransactionValues<'TKey,'TValue
                        when 'TKey: equality
                         and 'TValue: equality>
                         (backingStore:IDictionary<'TKey, 'TValue>,
                          lockStore:unit->unit,
                          finished:unit->unit) as this =     
    let transaction = 
        let txn = System.Transactions.Transaction.Current
        if txn = null then 
            let msg = "TransactionValues can only be created in the context of an open transaction"
            raise (System.InvalidOperationException(msg))
        txn.EnlistVolatile(this, EnlistmentOptions.None) |> ignore
        txn

    //We store the changes that happen during the transaction as a Dictionary<'TKey, 'TValue option>.
    //A value of (Some v) means that v was put in the dictionary for the given key, whereas a value
    //of None means the key was removed from the dictionary. No keys or values will be present
    //in this dictionary for keys in the backing store that have not been modified during the transaction.
    let transactionValues = 
        if backingStore.IsReadOnly then
            raise (System.ArgumentException("Source dictionary is ReadOnly"))
        new Dictionary<'TKey, 'TValue option>()

    //The undo data needed to undo changes if a rollback happens during 
    //a 2-phase commit. Generated during the IEnlistmentNotification.Prepare method.
    let mutable undo:seq<'TKey * 'TValue option> = Seq.empty

    //Tracks whether the Prepare method has been run or not. Under some circumstances,
    //Commit can be called without calling Prepare first, so Commit has to be be
    //able to do the work of Prepare as well.
    let mutable prepared = false

    let updateWithPair (dictionary:IDictionary<'a,'b>) (kv:'a * 'b option) =
        let (k,value) = kv
        match value with
        | Some v -> 
            dictionary.[k] <- v
        | None -> 
            dictionary.Remove k |> ignore

    let update (dictionary:IDictionary<'TKey,'TValue>) : seq<'TKey*'TValue option> =
        let undo = seq {
                        for kv in transactionValues do
                            let (has,old) = dictionary.TryGetValue(kv.Key)
                            updateWithPair dictionary (kv.Key, kv.Value)
                            yield kv.Key, (if has then Some old else None)
                            } |> List.ofSeq :> seq<'TKey *'TValue option>
        undo

    let prep() = lockStore()
                 undo <- update backingStore
                 prepared <- true

    let current () =
        let dict = new Dictionary<'TKey, 'TValue>(backingStore)
        update dict |> ignore
        seq { for kv in dict do yield kv }

    let get key = 
        let (succ, value) = transactionValues.TryGetValue(key)
        match (succ, value) with
        | true, ((Some _) as v) -> v
        | true, None -> None
        | false, _ -> match backingStore.TryGetValue(key) with
                      | true, v -> Some v
                      | false, _ -> None

    let getOrFail key =
        let v = get key
        if v.IsNone then
            raise (KeyNotFoundException())
        v.Value

    interface IDictionary<'TKey,'TValue>
        with
        member this.GetEnumerator() = 
            ((current() |> Seq.map (fun kv -> kv :> obj))
                :> System.Collections.IEnumerable).GetEnumerator()
        member this.GetEnumerator() = current().GetEnumerator()
        member this.Clear () =
            let keys = current() |> Seq.map (fun kv -> kv.Key) |> Array.ofSeq
            for key in keys do
                transactionValues.[key] <- None
        member this.Add(kv) = 
            transactionValues.[kv.Key] <- Some (kv.Value)
        member this.Contains(kv) = 
            let stored = get kv.Key
            stored.IsSome && stored.Value = kv.Value
        member this.Remove(kv:KeyValuePair<'TKey,'TValue>) =
            let existing = get kv.Key
            transactionValues.[kv.Key] <- None
            existing.IsSome && existing.Value = kv.Value
        member this.CopyTo(array, arrayIndex) = 
            let en = (this :> IEnumerable<KeyValuePair<'TKey,'TValue>>).GetEnumerator()
            for x = arrayIndex to (array.Length - 1) do
              if en.MoveNext() then
                array.[x] <- en.Current
            ()
        member this.Count with get () = current().Count()
        member this.IsReadOnly with get ()= false
        member this.Add(key, value) = transactionValues.[key] <- Some value
        member this.Item
                with get key = getOrFail key
                and  set key value = transactionValues.[key] <- Some value
        member this.ContainsKey key = get key |> Option.isSome
        member this.Remove (key:'TKey) = 
            let existing = get key
            transactionValues.[key] <- None
            existing.IsSome
        member this.TryGetValue(key:'TKey,value:byref<'TValue>) = 
            match transactionValues.TryGetValue(key) with
            | true,Some(v) -> value <- v; true
            | _ -> false

        member this.Keys = current() 
                            |> Seq.map (fun kv -> kv.Key) 
                            |> fun s -> new List<'TKey>(s) :> ICollection<'TKey>
        member this.Values = current() 
                            |> Seq.map (fun kv -> kv.Value) 
                            |> fun s -> new List<'TValue>(s) :> ICollection<'TValue>
                            
    interface System.Transactions.IEnlistmentNotification
        with
        member this.Commit(enlistment) = 
            if not prepared then
                prep()
            finished()
            enlistment.Done()
        member this.InDoubt(enlistment) = 
            enlistment.Done()
        member this.Prepare(enlistment) = 
            try 
                prep()
                enlistment.Prepared()
            with e -> enlistment.ForceRollback(e)
        member this.Rollback(enlistment) = 
            for kv in undo do
                updateWithPair backingStore kv
            undo <- Seq.empty
            finished()
            enlistment.Done()
        
///<summary>A Dictionary implementation that is transaction-aware</summary>
///Tracks changes that occur during a transaction and ensures that if the 
///transaction is rolled back, the dictionary will be unchanged. Threads that are participating
///in the transaction will see the changes as they are made, but threads that are in 
///other transactions, or no transaction, will not see the changes made in the transaction
///until it is committed.
///
///This dictionary, like System.Collections.Generic.Dictionary, is not thread safe,
///except that a thread that is in a transaction may safely use the it as the same time
///as another thread in a different transaction. Two threads in the same transaction,
///or threads that are not in a transaction, can cause the same kinds of issues that 
///arise with multithreaded use of System.Collections.Generic.Dictionary.
type TransactionalDictionary<'TKey, 'TValue 
        when 'TKey:equality
         and 'TValue: equality> 
            (backingStore:IDictionary<'TKey,'TValue>) = 

    let transactions = new Dictionary<Transaction,TransactionValues<'TKey, 'TValue>>()
    let sync = obj()
    let transactionLock = new TransactionLock()
    let getValues txn : IDictionary<'TKey, 'TValue> = 
        if txn = null then
            backingStore
        else
            let dict = lock sync (fun () ->
                                    let (containsKey,value) = transactions.TryGetValue txn
                                    match (containsKey,value) with
                                    | true, v -> v 
                                    | false, _ -> 
                                        let v = TransactionValues<'TKey, 'TValue>(
                                                            backingStore, 
                                                            (fun () -> transactionLock.Lock(txn)),
                                                            (fun () -> transactionLock.Unlock |> ignore
                                                                       lock sync (fun () -> transactions.Remove txn |> ignore)))
                                        transactions.[txn] <- v
                                        v)
            dict :> IDictionary<'TKey,'TValue>
    let rec getTxnValues () = getValues (Transaction.Current)
                      
    interface IDictionary<'TKey,'TValue>
        with
        member this.GetEnumerator() = 
            let tv = getTxnValues() :> System.Collections.IEnumerable
            tv.GetEnumerator()
        member this.GetEnumerator() = 
            let tv = getTxnValues() :> IEnumerable<KeyValuePair<'TKey,'TValue>>
            tv.GetEnumerator()
        member this.Clear () =
            let tv = getTxnValues() :> ICollection<KeyValuePair<'TKey,'TValue>>
            tv.Clear()
        member this.Add(kv) = 
            let tv = getTxnValues() :> ICollection<KeyValuePair<'TKey,'TValue>>
            tv.Add(kv)
        member this.Contains(kv) = 
            let tv = getTxnValues() :> ICollection<KeyValuePair<'TKey,'TValue>>
            tv.Contains(kv)
        member this.Remove(kv:KeyValuePair<'TKey,'TValue>) =
            let tv = getTxnValues() :> ICollection<KeyValuePair<'TKey,'TValue>>
            tv.Remove(kv)
        member this.CopyTo(array, arrayIndex) = 
            let tv = getTxnValues() :> ICollection<KeyValuePair<'TKey,'TValue>>
            tv.CopyTo(array, arrayIndex)
        member this.Count 
            with get () = 
                let tv = getTxnValues() :> ICollection<KeyValuePair<'TKey,'TValue>>
                tv.Count
        member this.IsReadOnly with get ()= false
        member this.Add(key, value) = 
            let tv = getTxnValues()
            tv.[key] <- value
        member this.Item
                with get key = let tv = getTxnValues() 
                               tv.[key]
                and  set key value = let tv = getTxnValues()
                                     tv.[key] <- value
        member this.Remove (key:'TKey) = 
            let tv = getTxnValues()
            tv.Remove key
        member this.TryGetValue(key:'TKey,value:byref<'TValue>) = 
            let tv = getTxnValues()
            tv.TryGetValue(key,&value)
        member this.ContainsKey(key:'TKey) = 
            let tv = getTxnValues()
            tv.ContainsKey key
        member this.Keys =
            let tv = getTxnValues()
            tv.Keys
        member this.Values = 
            let tv = getTxnValues()
            tv.Values

