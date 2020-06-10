using PageManager;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class PhyOpStaticRowProvider : IPhysicalOperator<Row>
    {
        private IEnumerable<Row> source;

        public PhyOpStaticRowProvider(IEnumerable<Row> rows)
        {
            this.source = rows;
        }

        public async IAsyncEnumerable<Row> Iterate(ITransaction _)
        {
            foreach (Row row in source)
            {
                yield return await Task.FromResult(row);
            }
        }

        public async Task Invoke()
        {
            await Task.FromResult(0);
        }
    }
}
