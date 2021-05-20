using MetadataManager;
using PageManager;
using System;
using System.Collections.Generic;
using System.Text;

namespace QueryProcessing
{
    /// <summary>
    /// Responsible for mapping function calls from syntax tree to functor class.
    /// </summary>
    public static class FuncCallMapper
    {
        public static MetadataColumn GetMetadataInfoForOutput(Sql.columnSelect.Func func, MetadataColumn[] sourceInput)
        {
            Sql.FuncType funcType = func.Item.Item1;

            if (funcType.IsAdd)
            {
                return AddFunctorOutputMappingHandler.GetMetadataInfoForOutput(func, sourceInput);
            }

            throw new NotImplementedException();
        }

        public static Action<RowHolder, RowHolder> BuildFunctor(Sql.columnSelect.Func func, int outputPosition, MetadataColumn[] sourceColumns)
        {
            Sql.FuncType funcType = func.Item.Item1;
            Sql.scalarArgs args = func.Item.Item2;

            if (funcType.IsAdd)
            {
                Sql.scalarArgs.Args2 argsExtracted = (Sql.scalarArgs.Args2)args;
                Sql.value arg1 = argsExtracted.Item.Item1;
                Sql.value arg2 = argsExtracted.Item.Item2;

                if (!arg1.IsId || !arg2.IsId)
                {
                    // TODO:
                    throw new Exception("Only support for ids as function arguments");
                }

                Sql.value.Id arg1Id = (Sql.value.Id)(arg1);
                Sql.value.Id arg2Id = (Sql.value.Id)(arg2);

                MetadataColumn mc1 = QueryProcessingAccessors.GetMetadataColumn(arg1Id.Item, sourceColumns);
                MetadataColumn mc2 = QueryProcessingAccessors.GetMetadataColumn(arg2Id.Item, sourceColumns);
                var functor = AddFunctorOutputMappingHandler.MapToFunctor(mc1, mc2);

                return (RowHolder inputRh, RowHolder outputRh) =>
                {
                    functor.ExecCompute(
                        inputRh,
                        outputRh,
                        new MetadataColumn[] { mc1, mc2 },
                        outputPosition
                    );
                };
            }

            throw new NotImplementedException();
        }
    }
}
