namespace Xoltar.Core.Transaction

open System.Collections.Generic
open System.Linq
open Microsoft.FSharp.Linq
open System.Transactions

type TransactionValues<'TKey,'TValue
                        when 'TKey: equality
                         and 'TValue: equality>
                         (backingStore:IDictionary<'TKey, 'TValue>,
                          finished:System.Action) as this =     
    let transaction = 
        let txn = System.Transactions.Transaction.Current
        if txn = null then 
            let msg = "TransactionalDictionary can only be created in the context of an open transaction"
            raise (System.InvalidOperationException(msg))
        txn.EnlistVolatile(this, System.Transactions.EnlistmentOptions.None) |> ignore
        txn

    let transactionValues = 
        if backingStore.IsReadOnly then
            raise (System.ArgumentException("Source dictionary is ReadOnly"))
        let dict = new Dictionary<'TKey, 'TValue option>()
        for kv in backingStore do
            dict.[kv.Key] <- Some kv.Value
        dict

    let remove key = transactionValues.[key] <- None
    let current () = seq {
                        for kv in transactionValues do
                        if Option.isSome (kv.Value) then 
                            yield new KeyValuePair<'TKey, 'TValue>(kv.Key, kv.Value.Value)
                            }
    let getWithDefault key = 
        let (succ, value) = transactionValues.TryGetValue(key)
        match (succ, value) with
        | true, ((Some _) as v) -> v
        | _ -> None

    let getOrFail key =
        let (succ, value) = transactionValues.TryGetValue(key)
        match (succ, value) with
        | true, (Some v) -> v
        | _ -> raise (KeyNotFoundException())

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
            let stored = getWithDefault kv.Key
            stored.IsSome && stored.Value = kv.Value
        member this.Remove(kv:KeyValuePair<'TKey,'TValue>) =
            let existing = getWithDefault kv.Key
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
        member this.ContainsKey key = getWithDefault key |> Option.isSome
        member this.Remove (key:'TKey) = 
            let existing = getWithDefault key
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
            for kv in transactionValues do
                match kv.Value with
                | Some v -> backingStore.[kv.Key] <- v
                | None -> backingStore.Remove kv.Key |> ignore
            enlistment.Done()
        member this.InDoubt(enlistment) = enlistment.Done()
        member this.Prepare(enlistment) = enlistment.Prepared()
        member this.Rollback(enlistment) = enlistment.Done()

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
    let transactionEnded txn = transactions.Remove txn |> ignore
    let getTxnValues txn : IDictionary<'TKey, 'TValue> = 
        if txn = null then
            backingStore
        else
            let (containsKey,value) = transactions.TryGetValue txn
            let dict = 
                match (containsKey,value) with
                | true, v -> v 
                | false, _ -> let v = TransactionValues<'TKey, 'TValue>(backingStore, fun () -> transactionEnded txn)
                              transactions.[txn] <- v
                              v
            dict :> IDictionary<'TKey,'TValue>
    let getTxnValues () = getTxnValues (Transaction.Current)
                      
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

