using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using Xoltar.Core.Transaction;
using Xunit;

namespace Xoltar.Core.CSharp.Test
{
    public class TransactionalDictionary
    {
        [Fact]
        public void after_commit_transaction_values_persist_to_the_backing_store()
        {
            var back = new Dictionary<int,int>();
            IDictionary<int,int> d = new TransactionalDictionary<int,int>(back);
            d[1] = 2;
            using (var txn = new TransactionScope())
            {
                d[1] = 5;
                txn.Complete();
            }
            Assert.Equal(5, back[1]);
        }
    }
}
