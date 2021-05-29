using MetadataManager;
using Microsoft.FSharp.Core;
using PageManager;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace QueryProcessing
{
    class FilterStatementBuilder : IStatementTreeBuilder
    {
        private static IComparable ValToIComp(Sql.valueOrFunc op, ref MetadataColumn[] metadataColumns, ref RowHolder rowHolder, InputStringNormalizer stringNormalizer)
        {
            if (op.IsValue)
            {
                Sql.value val = ((Sql.valueOrFunc.Value)op).Item;
                if (val.IsId)
                {
                    Sql.value.Id idVal = (Sql.value.Id)val;
                    // TODO: Metadata search shouldn't happen here.
                    MetadataColumn mc = QueryProcessingAccessors.GetMetadataColumn(idVal.Item, metadataColumns);
                    return QueryProcessingAccessors.MetadataColumnRowsetHolderFetcher(mc, rowHolder);
                }
                else if (val.IsFloat)
                {
                    return ((Sql.value.Float)val).Item;
                }
                else if (val.IsInt)
                {
                    return ((Sql.value.Int)val).Item;
                }
                else if (val.IsString)
                {
                    // TODO: This shouldn't happen here.
                    Sql.value normalizedString = stringNormalizer.ApplyReplacementTokens(val);
                    return ((Sql.value.String)normalizedString).Item;
                }
                else
                {
                    throw new InvalidProgramException("Invalid type.");
                }
            }
            else if (op.IsFuncCall)
            {
                var func = ((Sql.valueOrFunc.FuncCall)op).Item;
                string funcName = func.Item1;
                Sql.scalarArgs args = func.Item2;

                // TODO: There should be only one Func. Not one under project and one under valueOrFunc...
                Sql.columnSelect.Func columnSelectFunc = (Sql.columnSelect.Func)Sql.columnSelect.Func.NewFunc(func);

                // TODO: Again, this shouldn't happen for individual rows.
                // Instead function should be returned.
                return FuncCallMapper.BuildResultFunctor(columnSelectFunc, metadataColumns)(rowHolder);
            }

            throw new InvalidProgramException("Invalid state.");
        }

        public static Func<RowHolder, bool> EvalWhere(Sql.where where, MetadataColumn[] metadataColumns, InputStringNormalizer stringNormalizer)
        {
            // TODO: This is all functional programming and nice
            // but I really doubt the perf.
            // Recursion + higher order functions is probably super slow...

            Func<RowHolder, bool> returnFilterFunc = null;

            // TODO: Replacement Token and ValToIComp should go outside of the builder.
            returnFilterFunc = (rowHolder) =>
            {
                if (where.IsAnd)
                {
                    Sql.where.And andStmt = (Sql.where.And)where;

                    Func<RowHolder, bool> leftOp = EvalWhere(andStmt.Item1, metadataColumns, stringNormalizer);
                    Func<RowHolder, bool> rightOp = EvalWhere(andStmt.Item2, metadataColumns, stringNormalizer);

                    return leftOp(rowHolder) && rightOp(rowHolder);
                }
                else if (where.IsOr)
                {
                    Sql.where.Or orStmt = (Sql.where.Or)where;

                    Func<RowHolder, bool> leftOp = EvalWhere(orStmt.Item1, metadataColumns, stringNormalizer);
                    Func<RowHolder, bool> rightOp = EvalWhere(orStmt.Item2, metadataColumns, stringNormalizer);

                    return leftOp(rowHolder) || rightOp(rowHolder);
                }
                else if (where.IsCond)
                {
                    Sql.where.Cond condStmt = (Sql.where.Cond)where;

                    Sql.op op = condStmt.Item.Item2;

                    Sql.valueOrFunc leftOpValueOrFunc = condStmt.Item.Item1;
                    Sql.valueOrFunc rightOpValueOrFunc = condStmt.Item.Item3;

                    IComparable leftOpComp = ValToIComp(leftOpValueOrFunc, ref metadataColumns, ref rowHolder, stringNormalizer);
                    IComparable rightOpComp = ValToIComp(rightOpValueOrFunc, ref metadataColumns, ref rowHolder, stringNormalizer);

                    if (op.IsEq)
                    {
                        return leftOpComp.CompareTo(rightOpComp) == 0;
                    }
                    else if (op.IsGe)
                    {
                        return leftOpComp.CompareTo(rightOpComp) >= 0;
                    }
                    else if (op.IsGt)
                    {
                        return leftOpComp.CompareTo(rightOpComp) > 0;
                    }
                    else if (op.IsLe)
                    {
                        return leftOpComp.CompareTo(rightOpComp) <= 0;
                    }
                    else if (op.IsLt)
                    {
                        return leftOpComp.CompareTo(rightOpComp) < 0;
                    }
                }

                throw new InvalidProgramException("Invalid state.");
            };

            return returnFilterFunc;
        }

        public Task<IPhysicalOperator<RowHolder>> BuildStatement(Sql.sqlStatement statement, ITransaction tran, IPhysicalOperator<RowHolder> source, InputStringNormalizer inputStringNormalizer)
        {
            if (FSharpOption<Sql.where>.get_IsSome(statement.Where))
            {
                Sql.where whereStatement = statement.Where.Value;
                IPhysicalOperator<RowHolder> filterOp = new PhyOpFilter(source, FilterStatementBuilder.EvalWhere(whereStatement, source.GetOutputColumns(), inputStringNormalizer));
                return Task.FromResult(filterOp);
            }
            else
            {
                return Task.FromResult(source);
            }
        }
    }
}
