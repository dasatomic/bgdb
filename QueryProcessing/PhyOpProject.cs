using PageManager;
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

        public IEnumerable<Row> Iterate(ITransaction tran)
        {
            foreach (Row row in this.source.Iterate(tran))
            {
                yield return row.Project(this.columnChooser);
            }
        }

        public void Invoke()
        {
        }
    }
}
