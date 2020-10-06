using MetadataManager;
using PageManager;
using System;
using System.Diagnostics;
using System.Linq;

namespace QueryProcessing
{
    static class FilterStatementBuilder
    {
        private static IComparable ValToIComp(Sql.value op, ref MetadataColumn[] metadataColumns, ref RowHolder rowHolder)
        {
            if (op.IsId)
            {
                Sql.value.Id idVal = (Sql.value.Id)op;
                MetadataColumn mc = metadataColumns.First(mc => mc.ColumnName == idVal.Item);
                return QueryProcessingAccessors.MetadataColumnRowsetHolderFetcher(mc, rowHolder);
            }
            else if (op.IsFloat)
            {
                return ((Sql.value.Float)op).Item;
            }
            else if (op.IsInt)
            {
                return ((Sql.value.Int)op).Item;
            }
            else if (op.IsString)
            {
                return ((Sql.value.String)op).Item;
            }
            else
            {
                Debug.Fail("Invalid type");
            }

            throw new InvalidProgramException("Invalid state.");
        }

        public static Func<RowHolder, bool> EvalWhere(Sql.where where, MetadataColumn[] metadataColumns, InputStringNormalizer stringNormalizer)
        {
            // TODO: This is all functional programming and nice
            // but I really doubt the perf.
            // Recursion + higher order functions is probably super slow...

            Func<RowHolder, bool> returnFilterFunc = null;

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
                    Sql.value leftOp = stringNormalizer.ApplyReplacementTokens(condStmt.Item.Item1);
                    Sql.op op = condStmt.Item.Item2;
                    Sql.value rightOp = stringNormalizer.ApplyReplacementTokens(condStmt.Item.Item3);

                    IComparable leftOpComp, rightOpComp;

                    leftOpComp = ValToIComp(leftOp, ref metadataColumns, ref rowHolder);
                    rightOpComp = ValToIComp(rightOp, ref metadataColumns, ref rowHolder);

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
    }
}
