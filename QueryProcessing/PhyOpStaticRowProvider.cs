using PageManager;
using System;
using System.Collections;
using System.Collections.Generic;

namespace QueryProcessing
{
    public class PhyOpStaticRowProvider : IPhysicalOperator<Row>
    {
        private IEnumerable<Row> source;

        public PhyOpStaticRowProvider(IEnumerable<Row> rows)
        {
            this.source = rows;
        }

        public IEnumerable<Row> Iterate(ITransaction _)
        {
            return source;
        }

        public void Invoke()
        {
        }
    }
}
