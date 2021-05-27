using MetadataManager;
using PageManager;
using System;
using QueryProcessing.Utilities;
using QueryProcessing.Exceptions;

namespace QueryProcessing
{
    /// <summary>
    /// Responsible for mapping function calls from syntax tree to functor class.
    /// </summary>
    public static class FuncCallMapper
    {
        public static MetadataColumn GetMetadataInfoForOutput(Sql.columnSelect.Func func, MetadataColumn[] sourceInput)
        {
            string funcType = func.Item.Item1;

            if (funcType == "ADD")
            {
                return AddFunctorOutputMappingHandler.GetMetadataInfoForOutput(func, sourceInput);
            }

            throw new InvalidFunctionNameException();
        }

        private static int GetNumOfArguments(Sql.columnSelect.Func func)
        {
            if (func.Item.Item2.IsArgs1)
            {
                return 1;
            }
            else if (func.Item.Item2.IsArgs2)
            {
                return 2;
            }
            else if (func.Item.Item2.IsArgs3)
            {
                return 3;
            }
            else
            {
                throw new NotImplementedException("No support for 3+ arguments");
            }
        }

        private static Sql.value GetArgNum(int argNum, Sql.scalarArgs args)
        {
            if (args.IsArgs1)
            {
                if (argNum == 0)
                {
                    return ((Sql.scalarArgs.Args1)args).Item;
                }
                else
                {
                    throw new ArgumentException("Invalid argument requested");
                }
            }
            else if (args.IsArgs2)
            {
                if (argNum == 0)
                {
                    return ((Sql.scalarArgs.Args2)args).Item.Item1;
                }
                else if (argNum == 1)
                {
                    return ((Sql.scalarArgs.Args2)args).Item.Item2;
                }
                else
                {
                    throw new ArgumentException("Invalid argument requested");
                }
            }
            else if (args.IsArgs3)
            {
                if (argNum == 0)
                {
                    return ((Sql.scalarArgs.Args3)args).Item.Item1;
                }
                else if (argNum == 1)
                {
                    return ((Sql.scalarArgs.Args3)args).Item.Item2;
                }
                else if (argNum == 2)
                {
                    return ((Sql.scalarArgs.Args3)args).Item.Item3;
                }
                else
                {
                    throw new ArgumentException("Invalid argument requested");
                }
            }
            else
            {
                throw new NotImplementedException("Only up to 3 args supported");
            }
        }

        public static ColumnType[] ExtractCallTypes(Sql.columnSelect.Func func, MetadataColumn[] metadataColumns)
        {
            int numOfArguments = GetNumOfArguments(func);
            ColumnType[] result = new ColumnType[numOfArguments];

            for (int i = 0; i < numOfArguments; i++)
            {
                Sql.value value = GetArgNum(i, func.Item.Item2);

                if (value.IsId)
                {
                    string columnName = ((Sql.value.Id)value).Item;
                    MetadataColumn md = QueryProcessingAccessors.GetMetadataColumn(columnName, metadataColumns);
                    result[i] = md.ColumnType.ColumnType;
                }
                else
                {
                    result[i] = QueryProcessingAccessors.ValueToType(value);
                }
            }

            return result;
        }

        public static Action<RowHolder, RowHolder> BuildFunctor(Sql.columnSelect.Func func, int outputPosition, MetadataColumn[] sourceColumns)
        {
            string funcType = func.Item.Item1;
            Sql.scalarArgs args = func.Item.Item2;

            if (funcType == "ADD")
            {
                if (!args.IsArgs2)
                {
                    throw new ArgumentException("Invalid number of arguments for Add");
                }

                // TODO: This needs more refactoring. This shouldn't be part of Add call...
                int numOfArgumnets = GetNumOfArguments(func);
                Union2Type<MetadataColumn, Sql.value>[] fetchers = new Union2Type<MetadataColumn, Sql.value>[numOfArgumnets];
                ColumnType[] funcCallTypes = ExtractCallTypes(func, sourceColumns);
                var functor = AddFunctorOutputMappingHandler.MapToFunctor(funcCallTypes[0], funcCallTypes[1]);

                for (int argNum = 0; argNum < numOfArgumnets; argNum++)
                {
                    Sql.value arg = GetArgNum(argNum, args);
                    ColumnType argType = funcCallTypes[argNum];

                    if (arg.IsId)
                    {
                        Sql.value.Id idArg = (Sql.value.Id)(arg);
                        MetadataColumn mc = QueryProcessingAccessors.GetMetadataColumn(idArg.Item, sourceColumns);
                        fetchers[argNum] = new Union2Type<MetadataColumn, Sql.value>.Case1(mc);
                    }
                    else
                    {
                        fetchers[argNum] = new Union2Type<MetadataColumn, Sql.value>.Case2(arg);
                    }
                }

                return (RowHolder inputRh, RowHolder outputRh) =>
                {
                    functor.ExecCompute(
                        inputRh,
                        outputRh,
                        fetchers,
                        outputPosition
                    );
                };
            }

            throw new InvalidFunctionNameException();
        }
    }
}
