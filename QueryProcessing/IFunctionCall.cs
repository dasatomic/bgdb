using MetadataManager;
using PageManager;
using QueryProcessing.Utilities;

namespace QueryProcessing
{
    public interface IFunctionCall
    {
        void ExecCompute(RowHolder inputRowHolder, RowHolder outputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArguments, int outputPosition);
    }
}
