namespace Xoltar.Core.Transaction
open System.Collections.Generic
open System.Transactions
open System.Threading

//inspired by Juval Lowy's transaction lock at http://msdn.microsoft.com/en-us/magazine/cc163688.aspx
type TransactionLock() =
    let pending = LinkedList<Transaction*ManualResetEvent>()
    let mutable owner : Transaction = null
    let sync = new obj()
    with 
        member this.Owner 
            with get () = lock sync (fun () -> owner) 
            and set v = lock sync (fun () -> owner <- v)
        member this.Locked with get () = this.Owner <> null
        member this.Lock(transaction) = 
            Monitor.Enter sync
            if this.Owner = null then
                if transaction <> null then
                    this.Owner <- transaction
                Monitor.Exit sync
            else //Some transaction owns the lock
                if transaction = this.Owner then
                    Monitor.Exit sync
                else         //Otherwise, need to acquire the transaction lock
                    let manualEvent = new ManualResetEvent(false)
                    let pair = (transaction,manualEvent)
                    pending.AddLast pair |> ignore
                    if transaction <> null then
                      transaction.TransactionCompleted.Add (fun e ->
                        lock sync (fun() -> pending.Remove(pair)) |> ignore
                        lock manualEvent (fun() -> if not manualEvent.SafeWaitHandle.IsClosed then
                                                     manualEvent.Set() |> ignore))
                    Monitor.Exit sync
                    //Block the transaction or the calling thread
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
