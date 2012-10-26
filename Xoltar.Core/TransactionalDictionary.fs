namespace Xoltar.Core.Transaction

open System.Collections.Generic
open System.Linq
open Microsoft.FSharp.Linq
open System.Transactions
open System.Threading

type TransactionValues<'TKey,'TValue
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
        txn.EnlistVolatile(this :> IEnlistmentNotification, 
            System.Transactions.EnlistmentOptions.None) |> ignore
        txn

    let transactionValues = 
        if backingStore.IsReadOnly then
            raise (System.ArgumentException("Source dictionary is ReadOnly"))
        new Dictionary<'TKey, 'TValue option>()

    let mutable undo:seq<KeyValuePair<'TKey,'TValue option>> = Seq.empty
    let mutable prepared = false

    let remove key = transactionValues.[key] <- None

    let applyKv (dictionary:IDictionary<'a,'b>) (kv:KeyValuePair<'a,'b option>) =
        match kv.Value with
        | Some v -> 
            printfn "Setting %A to %A" kv.Key v
            dictionary.[kv.Key] <- v
        | None -> 
            printfn "Removing %A" kv.Key
            dictionary.Remove kv.Key |> ignore

    let kvp k v = KeyValuePair(k,v)

    let update (dictionary:IDictionary<'a,'b>) : seq<KeyValuePair<'a,'b option>> =
        let undo = seq {
                        for kv in transactionValues do
                            let (has,old) = dictionary.TryGetValue(kv.Key)
                            applyKv dictionary kv
                            yield kvp kv.Key (if has then Some old else None)
                            } |> Array.ofSeq |> Seq.ofArray
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
            keys |> Seq.iter remove
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
            System.Console.WriteLine "Commit"
            if not prepared then
                prep()
            finished()
            System.Console.WriteLine "Committed"
            enlistment.Done()
        member this.InDoubt(enlistment) = 
            System.Console.WriteLine "In doubt"
            enlistment.Done()
        member this.Prepare(enlistment) = 
            System.Console.WriteLine "Prepare"
            prep()
            System.Console.WriteLine "Prepared"
            enlistment.Prepared()
        member this.Rollback(enlistment) = 
            System.Console.WriteLine "Rollback"
            for kv in undo do
                applyKv backingStore kv
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
///it can only be safely used by a single thread at a time.
type TransactionalDictionary<'TKey, 'TValue 
        when 'TKey:equality
         and 'TValue: equality> 
            (backingStore:IDictionary<'TKey,'TValue>) = 

    let transactions = new Dictionary<Transaction,TransactionValues<'TKey, 'TValue>>()
    let sync = obj()
    let transactionLock = new TransactionLock()
    let [<VolatileField>]mutable inconsistent = false
    let getValues txn : IDictionary<'TKey, 'TValue> = 
        if txn = null then
            backingStore
        else
            let (containsKey,value) = lock sync (fun () -> transactions.TryGetValue txn)
            let dict = 
                match (containsKey,value) with
                | true, v -> v 
                | false, _ -> txn.TransactionCompleted.Add
                                        (fun e -> System.Console.WriteLine "Completed event"
                                                  transactionLock.Lock(txn) |> ignore
                                                  inconsistent <- false)
                              let v = TransactionValues<'TKey, 'TValue>(
                                        backingStore, 
                                        (fun () -> System.Console.WriteLine "Starting"
                                                   transactionLock.Lock(txn)
                                                   System.Console.WriteLine "Locked"
                                                   inconsistent <- false),
                                        (fun () -> 
                                                   System.Console.WriteLine "Finishing"
                                                   inconsistent <- false
                                                   transactionLock.Unlock |> ignore
                                                   System.Console.WriteLine "Unlocked"
                                                   lock sync (fun () -> transactions.Remove txn |> ignore)
                                                   ))
                              transactions.[txn] <- v
                              v
            dict :> IDictionary<'TKey,'TValue>
    let rec getTxnValues () = printfn "getTxnValues on thread %s" Thread.CurrentThread.Name
                              let txn = try Some (Transaction.Current)
                                        with :? System.InvalidOperationException -> None
                              let now = System.DateTime.Now
                              match txn with
                              | None -> printfn "Txn already completed at %A" now
                                        inconsistent <- true
                                        while inconsistent do
                                            Thread.Sleep(10)
                                        let finish= System.DateTime.Now.Subtract(now).TotalMilliseconds
                                        printfn "Done with sleep after %A ms" finish
                                        getValues null
                              | Some v -> getValues v
                      
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

