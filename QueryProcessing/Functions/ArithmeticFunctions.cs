using MetadataManager;
using PageManager;
using QueryProcessing.Utilities;
using QueryProcessing.Exceptions;
using System;

namespace QueryProcessing.Functions
{
    public class AddFunctorOutputMappingHandler : IFunctionMappingHandler
    {
        public MetadataColumn GetMetadataInfoForOutput(Sql.columnSelect.Func func, MetadataColumn[] metadataColumns)
        {
            ColumnType[] columnTypes = FuncCallMapper.ExtractCallTypes(func, metadataColumns);

            if (columnTypes.Length != 2)
            {
                throw new InvalidFunctionArgument("Add requires 2 arguments");
            }

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
            FunctorArgChecks.CheckInputArguments(sourceArguments, new[] { ColumnType.Int, ColumnType.Int });

            FunctorArgExtractIntInt args = new FunctorArgExtractIntInt(inputRowHolder, sourceArguments);
            int res = args.ArgOne + args.ArgTwo;
            outputRowHolder.SetField<int>(outputPosition, res);
        }

        public IComparable ExecCompute(RowHolder inputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArguments)
        {
            FunctorArgChecks.CheckInputArguments(sourceArguments, new[] { ColumnType.Int, ColumnType.Int });

            FunctorArgExtractIntInt args = new FunctorArgExtractIntInt(inputRowHolder, sourceArguments);
            return args.ArgOne + args.ArgTwo;
        }
    }

    public class AddFunctorDouble : IFunctionCall
    {
        public void ExecCompute(RowHolder inputRowHolder, RowHolder outputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArguments, int outputPosition)
        {
            FunctorArgChecks.CheckInputArguments(sourceArguments, new[] { ColumnType.Double, ColumnType.Double});

            FunctorArgExtractDoubleDouble args = new FunctorArgExtractDoubleDouble(inputRowHolder, sourceArguments);
            double res = args.ArgOne + args.ArgTwo;
            outputRowHolder.SetField<double>(outputPosition, res);
        }

        public IComparable ExecCompute(RowHolder inputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArguments)
        {
            FunctorArgChecks.CheckInputArguments(sourceArguments, new[] { ColumnType.Double, ColumnType.Double});

            FunctorArgExtractDoubleDouble args = new FunctorArgExtractDoubleDouble(inputRowHolder, sourceArguments);
            return args.ArgOne + args.ArgTwo;
        }
    }

    public class AddFunctorDoubleInt : IFunctionCall
    {
        public void ExecCompute(RowHolder inputRowHolder, RowHolder outputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArguments, int outputPosition)
        {
            FunctorArgChecks.CheckInputArguments(sourceArguments, new[] { ColumnType.Double, ColumnType.Int});

            FunctorArgExtractDoubleInt args = new FunctorArgExtractDoubleInt(inputRowHolder, sourceArguments);
            double res = args.ArgOne + args.ArgTwo;
            outputRowHolder.SetField<double>(outputPosition, res);
        }

        public IComparable ExecCompute(RowHolder inputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArguments)
        {
            FunctorArgChecks.CheckInputArguments(sourceArguments, new[] { ColumnType.Double, ColumnType.Int});

            FunctorArgExtractDoubleInt args = new FunctorArgExtractDoubleInt(inputRowHolder, sourceArguments);
            return args.ArgOne + args.ArgTwo;
        }
    }

    public class AddFunctorIntDouble : IFunctionCall
    {
        public void ExecCompute(RowHolder inputRowHolder, RowHolder outputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArguments, int outputPosition)
        {
            FunctorArgChecks.CheckInputArguments(sourceArguments, new[] { ColumnType.Int, ColumnType.Double});

            FunctorArgExtractIntDouble args = new FunctorArgExtractIntDouble(inputRowHolder, sourceArguments);
            double res = args.ArgOne + args.ArgTwo;
            outputRowHolder.SetField<double>(outputPosition, res);
        }

        public IComparable ExecCompute(RowHolder inputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArguments)
        {
            FunctorArgChecks.CheckInputArguments(sourceArguments, new[] { ColumnType.Int, ColumnType.Double});

            FunctorArgExtractIntDouble args = new FunctorArgExtractIntDouble(inputRowHolder, sourceArguments);
            return args.ArgOne + args.ArgTwo;
        }
    }
}
