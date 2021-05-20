using MetadataManager;
using PageManager;
using System;
using System.Linq;

namespace QueryProcessing
{
    static class FunctorArgChecks
    {
        public static void CheckInputArguments(MetadataColumn[] sourceArguments, ColumnType[] acceptedColumnTypes)
        {
            if (sourceArguments.Length != acceptedColumnTypes.Length)
            {
                throw new ArgumentException("Invalid number of arguments");
            }

            foreach (MetadataColumn md in sourceArguments)
            {
                if (!acceptedColumnTypes.Any(acc => md.ColumnType.ColumnType == acc))
                {
                    throw new ArgumentException($"Type {md.ColumnType.ColumnType} is not accepted by Add functor");
                }
            }
        }
    }

    public static class AddFunctorOutputMappingHandler
    {
        public static MetadataColumn GetMetadataInfoForOutput(Sql.columnSelect.Func func, MetadataColumn[] metadataColumns)
        {
            Sql.scalarArgs args = func.Item.Item2;

            if (!args.IsArgs2)
            {
                throw new ArgumentException("Add accepts two args");
            }

            var args2 = ((Sql.scalarArgs.Args2)args).Item;
            Sql.value argOne = args2.Item1;
            Sql.value argTwo = args2.Item2;

            if (!argOne.IsId || !argTwo.IsId)
            {
                throw new NotImplementedException("Currently we only support ids in as arguments");
            }

            Sql.value.Id idOne = (Sql.value.Id)argOne;
            Sql.value.Id idTwo = (Sql.value.Id)argTwo;

            ColumnInfo argOneMd = QueryProcessingAccessors.GetMetadataColumn(idOne.Item, metadataColumns).ColumnType;
            ColumnInfo argTwoMd = QueryProcessingAccessors.GetMetadataColumn(idTwo.Item, metadataColumns).ColumnType;

            // both need to be double or int.
            // TODO: Need generic way to express this.
            if (!((argOneMd.ColumnType == ColumnType.Double || argOneMd.ColumnType == ColumnType.Int) &&
                argTwoMd.ColumnType == ColumnType.Double || argTwoMd.ColumnType == ColumnType.Int))
            {
                throw new ArgumentException("Invalid argument type for add");
            }

            if (argOneMd.ColumnType == ColumnType.Double || argTwoMd.ColumnType == ColumnType.Double)
            {
                // If one of them is double map result to double.
                return new MetadataColumn(0, 0, "ADD_Result", new ColumnInfo(ColumnType.Double));
            }
            else
            {
                return new MetadataColumn(0, 0, "ADD_Result", new ColumnInfo(ColumnType.Int));
            }
        }

        public static IFunctionCall MapToFunctor(MetadataColumn arg1, MetadataColumn arg2)
        {
            return ((arg1.ColumnType.ColumnType, arg2.ColumnType.ColumnType)) switch
            {
                (ColumnType.Int, ColumnType.Int) => new AddFunctorInt(),
                (ColumnType.Int, ColumnType.Double) => new AddFunctorIntDouble(),
                (ColumnType.Double, ColumnType.Int) => new AddFunctorDoubleInt(),
                (ColumnType.Double, ColumnType.Double) => new AddFunctorDouble(),
                _ => throw new ArgumentException("Invalid type"),
            };
        }
    }

    public class AddFunctorInt : IFunctionCall
    {
        public void ExecCompute(RowHolder inputRowHolder, RowHolder outputRowHolder, MetadataColumn[] sourceArguments, int outputPosition)
        {
            FunctorArgChecks.CheckInputArguments(sourceArguments, new[] { ColumnType.Int, ColumnType.Int });
            int argOneExtracted = inputRowHolder.GetField<int>(sourceArguments[0].ColumnId);
            int argTwoExtracted = inputRowHolder.GetField<int>(sourceArguments[1].ColumnId);

            int res = argOneExtracted + argTwoExtracted;

            outputRowHolder.SetField<int>(outputPosition, res);
        }
    }

    public class AddFunctorDouble : IFunctionCall
    {
        public void ExecCompute(RowHolder inputRowHolder, RowHolder outputRowHolder, MetadataColumn[] sourceArguments, int outputPosition)
        {
            FunctorArgChecks.CheckInputArguments(sourceArguments, new[] { ColumnType.Double, ColumnType.Double});

            double argOneExtracted = inputRowHolder.GetField<double>(sourceArguments[0].ColumnId);
            double argTwoExtracted = inputRowHolder.GetField<double>(sourceArguments[1].ColumnId);

            double res = argOneExtracted + argTwoExtracted;

            outputRowHolder.SetField<double>(outputPosition, res);
        }
    }

    public class AddFunctorDoubleInt : IFunctionCall
    {
        public void ExecCompute(RowHolder inputRowHolder, RowHolder outputRowHolder, MetadataColumn[] sourceArguments, int outputPosition)
        {
            FunctorArgChecks.CheckInputArguments(sourceArguments, new[] { ColumnType.Double, ColumnType.Int});

            double argOneExtracted = inputRowHolder.GetField<double>(sourceArguments[0].ColumnId);
            double argTwoExtracted = inputRowHolder.GetField<int>(sourceArguments[1].ColumnId);

            double res = argOneExtracted + argTwoExtracted;

            outputRowHolder.SetField<double>(outputPosition, res);
        }
    }

    public class AddFunctorIntDouble : IFunctionCall
    {
        public void ExecCompute(RowHolder inputRowHolder, RowHolder outputRowHolder, MetadataColumn[] sourceArguments, int outputPosition)
        {
            FunctorArgChecks.CheckInputArguments(sourceArguments, new[] { ColumnType.Double, ColumnType.Int});

            double argOneExtracted = inputRowHolder.GetField<int>(sourceArguments[0].ColumnId);
            double argTwoExtracted = inputRowHolder.GetField<double>(sourceArguments[1].ColumnId);

            double res = argOneExtracted + argTwoExtracted;

            outputRowHolder.SetField<double>(outputPosition, res);
        }
    }
}
