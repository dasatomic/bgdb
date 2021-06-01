using MetadataManager;
using PageManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class PhyOpProjectComputeFunctors
    {
        public Func<RowHolder, RowHolder> Projector { get; }
        public Action<RowHolder, RowHolder> Computer { get; }

        public PhyOpProjectComputeFunctors(
            Func<RowHolder, RowHolder> projector,
            Action<RowHolder, RowHolder> computer)
        {
            this.Projector = projector;
            this.Computer = computer;
        }
    }

    public class PhyOpProject : IPhysicalOperator<RowHolder>
    {
        private IPhysicalOperator<RowHolder> source;
        private PhyOpProjectComputeFunctors functors;
        private MetadataColumn[] outputMd;
        bool isStar = false;
        int? topRows;

        public PhyOpProject(IPhysicalOperator<RowHolder> source, PhyOpProjectComputeFunctors functors, MetadataColumn[] outputMd, int? topRows)
        {
            this.source = source;
            this.functors = functors;
            this.topRows = topRows;
            this.isStar = false;
            this.outputMd = outputMd;
        }

        public PhyOpProject(IPhysicalOperator<RowHolder> source, int? topRows)
        {
            this.source = source;
            this.topRows = topRows;
            this.isStar = true;
            this.outputMd = source.GetOutputColumns();
        }

        public async IAsyncEnumerable<RowHolder> Iterate(ITransaction tran)
        {
            await foreach (RowHolder row in this.source.Iterate(tran))
            {
                if (this.isStar)
                {
                    yield return row;
                }
                else
                {
                    RowHolder project = this.functors.Projector(row);
                    this.functors.Computer(row, project);
                    yield return project;
                }

                if (--this.topRows == 0)
                {
                    yield break;
                }
            }
        }

        public async Task Invoke()
        {
            await Task.FromResult(0);
        }

        public MetadataColumn[] GetOutputColumns() => outputMd;
    }
}
