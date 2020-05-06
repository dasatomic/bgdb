using System.Collections;
using System.Collections.Generic;

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
            foreach (Row row in this.source)
            {
                yield return row.Project(this.columnChooser);
            }
        }

        public void Invoke()
        {
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator1();
        }

        private IEnumerator GetEnumerator1()
        {
            return this.GetEnumerator();
        }
    }
}
