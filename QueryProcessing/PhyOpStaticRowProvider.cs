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

        public IEnumerator<Row> GetEnumerator()
        {
            return source.GetEnumerator();
        }

        public void Invoke()
        {
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return source.GetEnumerator();
        }
    }
}
