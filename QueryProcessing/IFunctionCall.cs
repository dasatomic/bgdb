using MetadataManager;
using PageManager;
using QueryProcessing.Utilities;
using System;

namespace QueryProcessing
{
    public interface IFunctionCall
    {
        void ExecCompute(RowHolder inputRowHolder, RowHolder outputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArguments, int outputPosition);
        IComparable ExecCompute(RowHolder inputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArguments);
    }

    public interface IFunctionMappingHandler
    {
        MetadataColumn GetMetadataInfoForOutput(Sql.columnSelect.Func func, MetadataColumn[] metadataColumns);
        public IFunctionCall MapToFunctor(ColumnType[] args);
    }
}
