namespace Xoltar.Core.Transaction.Test
open Xunit
open Xoltar.Core.Transaction
open System.Collections.Generic
open System.Transactions

module Dictionary =
    let dict () = Dictionary()
    let trans<'k,'v when 'k:equality and 'v:equality>() = 
        let d = dict()
        TransactionalDictionary<'k,'v>(d) :> IDictionary<'k,'v>, d
    let slim set = new System.Threading.ManualResetEventSlim(set)
    [<Fact>]
    let ``a new dictionary has count = 0``() = 
        let d,back = trans()
        Assert.Equal(0, d.Count)  

    [<Fact>]
    let ``after adding an item, the item can be retrieved``()= 
        let d,back = trans()
        d.[1] <- 2
        Assert.Equal(2, d.[1])

    [<Fact>]
    let ``after adding an item, then clearing, the count = 0``() =
        let d,back = trans()
        d.[1] <- 2
        d.Clear()
        Assert.Equal(0, d.Count)

    [<Fact>]
    let ``after adding an item, then removing, the count = 0``() =
        let d,back = trans()
        d.[1] <- 2
        d.Remove(1) |> ignore
        Assert.Equal(0, d.Count)

    [<Fact>]
    let ``iterating works outside a transaction``() =
        let d,back = trans()
        d.[1] <- 2
        d.[2] <- 3
        Assert.Equal(5, d.Values |> Seq.sum )
        Assert.Equal(2, d.Values |> Seq.length )
        
    [<Fact>]
    let ``iterating works inside a transaction``() =
        let d,back = trans()
        d.[1] <- 2
        using (new TransactionScope()) (fun txn ->
            d.[2] <- 3
            Assert.Equal(5, d.Values |> Seq.sum )
            Assert.Equal(2, d.Values |> Seq.length )
        )

    [<Fact>]
    let ``iterating keys works inside a transaction``() =
        let d,back = trans()
        d.[1] <- 2
        using (new TransactionScope()) (fun txn ->
            d.[2] <- 3
            Assert.Equal(3, d.Keys |> Seq.sum )
            Assert.Equal(2, d.Keys |> Seq.length )
        )

    [<Fact>]
    let ``values in a transaction are isolated``() = 
        use valueSet = slim false 
        use valueChecked = slim false
        (
            let d,back = trans()
            d.[1] <- 2
            let t = System.Threading.Tasks.Task.Factory.StartNew (fun () -> 
                        valueSet.Wait()
                        try
                            Assert.Equal(2, d.[1])
                        finally
                            valueChecked.Set())
            use txn = new System.Transactions.TransactionScope()
            (
                d.[1] <- 5
                valueSet.Set()
                valueChecked.Wait()
            )
            t.Wait()
        )

    [<Fact>]
    let ``values in a transaction are visible to the transaction``() = 
        let d,back = trans()
        d.[1] <- 2
        use txn = new System.Transactions.TransactionScope()
        (
            d.[1] <- 5
            Assert.Equal(5, d.[1])
        )
    
    [<Fact>]
    let ``after rollback, transaction values are not committed``()= 
        let d,back = trans()
        d.[1] <- 2
        using(new System.Transactions.TransactionScope()) (fun txn ->
            d.[1] <- 5
        )
        Assert.Equal(2, back.[1])

    [<Fact>]
    let ``after rollback, transaction values are gone in trans dict``()= 
        let d,back = trans()
        d.[1] <- 2
        using(new System.Transactions.TransactionScope()) (fun txn ->
            d.[1] <- 5
        )
        Assert.Equal(2, d.[1])

    [<Fact>][<Trait("runthis","yes")>]
    let ``after commit, transaction values persist``()= 
        let d,back = trans()
        d.[1] <- 2
        Assert.Null System.Transactions.Transaction.Current
        using (new System.Transactions.TransactionScope()) (fun txn ->
            d.[1] <- 5
            txn.Complete()
        )
        let result = d.[1]
        Assert.Equal(5, result)

    [<Fact>]
    let ``after commit, transaction values persist to the backing store``()= 
        let d,back = trans()
        d.[1] <- 2
        using (new System.Transactions.TransactionScope()) (fun txn ->
            d.[1] <- 5
            txn.Complete()
        )
        Assert.Equal(5, back.[1])
    
    [<Fact>]
    let ``last commit wins``() = 
        use valueSet = slim false 
        use valueSet2 = slim false
        use valueChecked = slim false
        (
            let d,back = trans()
            d.[1] <- 2
            let t = System.Threading.Tasks.Task.Factory.StartNew (fun () -> 
                        valueSet.Wait()
                        try
                            Assert.Equal(2, d.[1])
                        finally
                            valueChecked.Set())
            using(new System.Transactions.TransactionScope()) (fun txn ->
                d.[1] <- 5
                valueSet.Set()
                valueChecked.Wait()
                txn.Complete()
            )
            t.Wait()
            Assert.Equal(5, back.[1])
        )