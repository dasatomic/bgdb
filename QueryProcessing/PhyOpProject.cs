using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace QueryProcessing
{
    public class PhyOpProject : IPhysicalOperator<Row>
    {
        private IPhysicalOperator<Row> source;
        private int[] columnChooser;

        public PhyOpProject(IPhysicalOperator<Row> source, int[] columnChooser)
        {
            this.source = source;
            this.columnChooser = columnChooser;
        }

        public IEnumerator<Row> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public void Invoke()
        {
            throw new NotImplementedException();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }
}
