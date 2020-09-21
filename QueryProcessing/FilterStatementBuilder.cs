﻿using MetadataManager;
using PageManager;
using System;
using System.Diagnostics;
using System.Linq;

namespace QueryProcessing
{
    static class FilterStatementBuilder
    {
        private static IComparable ValToIComp(Sql.value op, ref MetadataColumn[] metadataColumns, ref RowHolderFixed rowHolder)
        {
            if (op.IsId)
            {
                Sql.value.Id idVal = (Sql.value.Id)op;
                MetadataColumn mc = metadataColumns.First(mc => mc.ColumnName == idVal.Item);

                if (mc.ColumnType.ColumnType == ColumnType.Int)
                {
                    return rowHolder.GetField<int>(mc.ColumnId);
                }
                else if (mc.ColumnType.ColumnType == ColumnType.Double)
                {
                    return rowHolder.GetField<double>(mc.ColumnId);
                }
                else if (mc.ColumnType.ColumnType == ColumnType.String)
                {
                    // TODO: Since char[] doesn't implement IComparable need to cast it to string.
                    // This is super slow...
                    // Consider creating your own string type.
                    return new string(rowHolder.GetStringField(mc.ColumnId));
                }
                else
                {
                    Debug.Fail("Invalid column type");
                }
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

        public static Func<RowHolderFixed, bool> EvalWhere(Sql.where where, MetadataColumn[] metadataColumns)
        {
            // TODO: This is all functional programming and nice
            // but I really doubt the perf.
            // Recursion + higher order functions is probably super slow...

            Func<RowHolderFixed, bool> returnFilterFunc = null;

            returnFilterFunc = (rowHolder) =>
            {
                if (where.IsAnd)
                {
                    Sql.where.And andStmt = (Sql.where.And)where;

                    Func<RowHolderFixed, bool> leftOp = EvalWhere(andStmt.Item1, metadataColumns);
                    Func<RowHolderFixed, bool> rightOp = EvalWhere(andStmt.Item2, metadataColumns);

                    return leftOp(rowHolder) && rightOp(rowHolder);
                }
                else if (where.IsOr)
                {
                    Sql.where.Or orStmt = (Sql.where.Or)where;

                    Func<RowHolderFixed, bool> leftOp = EvalWhere(orStmt.Item1, metadataColumns);
                    Func<RowHolderFixed, bool> rightOp = EvalWhere(orStmt.Item2, metadataColumns);

                    return leftOp(rowHolder) || rightOp(rowHolder);
                }
                else if (where.IsCond)
                {
                    Sql.where.Cond condStmt = (Sql.where.Cond)where;
                    Sql.value leftOp = condStmt.Item.Item1;
                    Sql.op op = condStmt.Item.Item2;
                    Sql.value rightOp = condStmt.Item.Item3;

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