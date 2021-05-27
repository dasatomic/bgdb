using MetadataManager;
using PageManager;
using QueryProcessing.Utilities;
using QueryProcessing.Exceptions;
using System;

namespace QueryProcessing
{
    static class FunctorArgChecks
    {
        public static void CheckInputArguments(Union2Type<MetadataColumn, Sql.value>[] sourceArguments, ColumnType[] acceptedColumnTypes)
        {
            if (sourceArguments.Length != acceptedColumnTypes.Length)
            {
                throw new InvalidFunctionArgument("Invalid number of arguments");
            }

            for (int i = 0; i < sourceArguments.Length; i++)
            {
                if (!sourceArguments[i].Match<bool>(
                    (md) => md.ColumnType.ColumnType == acceptedColumnTypes[i],
                    (val) => QueryProcessingAccessors.ValueToType(val) == acceptedColumnTypes[i]))
                {
                    throw new InvalidFunctionArgument($"This type is not accepted by this function.");
                }
            }
        }
    }

    public class AddFunctorOutputMappingHandler : IFunctionMappingHandler
    {
        public MetadataColumn GetMetadataInfoForOutput(Sql.columnSelect.Func func, MetadataColumn[] metadataColumns)
        {
            ColumnType[] columnTypes = FuncCallMapper.ExtractCallTypes(func, metadataColumns);

            return (columnTypes[0], columnTypes[1]) switch
            {
                (ColumnType.Double, ColumnType.Double) => new MetadataColumn(0, 0, "ADD_Result", new ColumnInfo(ColumnType.Double)),
                (ColumnType.Double, ColumnType.Int) => new MetadataColumn(0, 0, "ADD_Result", new ColumnInfo(ColumnType.Double)),
                (ColumnType.Int, ColumnType.Double) => new MetadataColumn(0, 0, "ADD_Result", new ColumnInfo(ColumnType.Double)),
                (ColumnType.Int, ColumnType.Int) => new MetadataColumn(0, 0, "ADD_Result", new ColumnInfo(ColumnType.Int)),
                _ => throw new InvalidFunctionArgument("invalid argument type for add")
            };
        }

        public IFunctionCall MapToFunctor(ColumnType[] args)
        {
            if (args.Length != 2)
            {
                throw new InvalidFunctionArgument("Add requires 2 arguments");
            }

            return (args[0], args[1]) switch
            {
                (ColumnType.Int, ColumnType.Int) => new AddFunctorInt(),
                (ColumnType.Int, ColumnType.Double) => new AddFunctorIntDouble(),
                (ColumnType.Double, ColumnType.Int) => new AddFunctorDoubleInt(),
                (ColumnType.Double, ColumnType.Double) => new AddFunctorDouble(),
                _ => throw new InvalidFunctionArgument("Invalid type"),
            };
        }
    }

    public class AddFunctorInt : IFunctionCall
    {
        public void ExecCompute(RowHolder inputRowHolder, RowHolder outputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArguments, int outputPosition)
        {
            int argOneExtracted = sourceArguments[0].Match<int>(
                (MetadataColumn md) => inputRowHolder.GetField<int>(md.ColumnId),
                (Sql.value val) => ((Sql.value.Int)val).Item);
            int argTwoExtracted = sourceArguments[1].Match<int>(
                (MetadataColumn md) => inputRowHolder.GetField<int>(md.ColumnId),
                (Sql.value val) => ((Sql.value.Int)val).Item);

            int res = argOneExtracted + argTwoExtracted;

            outputRowHolder.SetField<int>(outputPosition, res);
        }
    }

    public class AddFunctorDouble : IFunctionCall
    {
        public void ExecCompute(RowHolder inputRowHolder, RowHolder outputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArguments, int outputPosition)
        {
            FunctorArgChecks.CheckInputArguments(sourceArguments, new[] { ColumnType.Double, ColumnType.Double});

            double argOneExtracted = sourceArguments[0].Match<double>(
                (MetadataColumn md) => inputRowHolder.GetField<double>(md.ColumnId),
                (Sql.value val) => ((Sql.value.Float)val).Item);

            double argTwoExtracted = sourceArguments[1].Match<double>(
                (MetadataColumn md) => inputRowHolder.GetField<double>(md.ColumnId),
                (Sql.value val) => ((Sql.value.Float)val).Item);

            double res = argOneExtracted + argTwoExtracted;

            outputRowHolder.SetField<double>(outputPosition, res);
        }
    }

    public class AddFunctorDoubleInt : IFunctionCall
    {
        public void ExecCompute(RowHolder inputRowHolder, RowHolder outputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArguments, int outputPosition)
        {
            FunctorArgChecks.CheckInputArguments(sourceArguments, new[] { ColumnType.Double, ColumnType.Int});

            double argOneExtracted = sourceArguments[0].Match<double>(
                (MetadataColumn md) => inputRowHolder.GetField<double>(md.ColumnId),
                (Sql.value val) => ((Sql.value.Float)val).Item);

            int argTwoExtracted = sourceArguments[1].Match<int>(
                (MetadataColumn md) => inputRowHolder.GetField<int>(md.ColumnId),
                (Sql.value val) => ((Sql.value.Int)val).Item);

            double res = argOneExtracted + argTwoExtracted;

            outputRowHolder.SetField<double>(outputPosition, res);
        }
    }

    public class AddFunctorIntDouble : IFunctionCall
    {
        public void ExecCompute(RowHolder inputRowHolder, RowHolder outputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArguments, int outputPosition)
        {
            FunctorArgChecks.CheckInputArguments(sourceArguments, new[] { ColumnType.Int, ColumnType.Double});

            int argOneExtracted = sourceArguments[0].Match<int>(
                (MetadataColumn md) => inputRowHolder.GetField<int>(md.ColumnId),
                (Sql.value val) => ((Sql.value.Int)val).Item);

            double argTwoExtracted = sourceArguments[1].Match<double>(
                (MetadataColumn md) => inputRowHolder.GetField<double>(md.ColumnId),
                (Sql.value val) => ((Sql.value.Float)val).Item);


            double res = argOneExtracted + argTwoExtracted;

            outputRowHolder.SetField<double>(outputPosition, res);
        }
    }
}
