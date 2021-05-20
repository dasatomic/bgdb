using MetadataManager;
using PageManager;

namespace QueryProcessing
{
    public interface IFunctionCall
    {
        void ExecCompute(RowHolder inputRowHolder, RowHolder outputRowHolder, MetadataColumn[] sourceArguments, int outputPosition);
    }
}
