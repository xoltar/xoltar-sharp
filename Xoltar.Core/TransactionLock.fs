namespace Xoltar.Core.Transaction
open System.Collections.Generic
open System.Transactions
open System.Threading

//Based on Juval Lowy's transaction lock at http://msdn.microsoft.com/en-us/magazine/cc163688.aspx
type TransactionLock() =
    let pending = LinkedList<Transaction*ManualResetEvent>()
    let mutable owner : Transaction = null
    let sync = obj()
    member this.Owner 
        with get () = lock sync (fun () -> owner) 
        and set v = lock sync (fun () -> owner <- v)
    member this.Locked with get () = this.Owner <> null
    member this.Lock(transaction) = 
        let evt = lock sync (fun() -> 
                                if this.Owner = null then
                                    if transaction <> null then
                                        this.Owner <- transaction
                                    None
                                else                 
                                    if transaction <> this.Owner then
                                        let manualEvent = new ManualResetEvent(false)
                                        let pair = (transaction,manualEvent)
                                        pending.AddLast pair |> ignore
                                        if transaction <> null then
                                            transaction.TransactionCompleted.Add (fun e ->
                                            lock sync (fun() -> pending.Remove(pair)) |> ignore
                                            lock manualEvent (fun() -> if not manualEvent.SafeWaitHandle.IsClosed then
                                                                            manualEvent.Set() |> ignore))
                                        (Some manualEvent)
                                    else None)
        match evt with 
        | None -> ()
        | (Some manualEvent) -> 
                manualEvent.WaitOne() |> ignore
                lock(manualEvent) manualEvent.Close

    member this.Unlock () = 
        lock(sync) (fun () ->
            this.Owner <- null
            let mutable node : LinkedListNode<Transaction*ManualResetEvent> = null
            if pending.Count > 0 then
                node <- pending.First
                pending.RemoveFirst()
            if node <> null then
                let (transaction, manualEvent) = node.Value
                this.Lock(transaction)
                lock (manualEvent) (fun() -> if not manualEvent.SafeWaitHandle.IsClosed 
                                                then manualEvent.Set() |> ignore))
