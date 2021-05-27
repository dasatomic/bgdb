using MetadataManager;
using PageManager;
using System;
using QueryProcessing.Utilities;
using QueryProcessing.Exceptions;
using System.Collections.Generic;

namespace QueryProcessing
{
    /// <summary>
    /// Responsible for mapping function calls from syntax tree to functor class.
    /// </summary>
    public static class FuncCallMapper
    {
        private struct MetadataOutputFunctorBuilderPair
        {
            public Func<Sql.columnSelect.Func, MetadataColumn[] /* source columns */, MetadataColumn /* ret - output type */> GetMetadataInfoForOutput;
            public Func<Sql.columnSelect.Func, int /* output position */, MetadataColumn[] /* source columns */, Action<RowHolder, RowHolder> /* ret - Action mapper */> FunctorBuilder;
        }

        private static Dictionary<string, MetadataOutputFunctorBuilderPair> FuncDictionary = new Dictionary<string, MetadataOutputFunctorBuilderPair>()
        {
            {  "ADD", new MetadataOutputFunctorBuilderPair()
                {
                    GetMetadataInfoForOutput = (func, mds) => AddFunctorOutputMappingHandler.GetMetadataInfoForOutput(func, mds),
                    FunctorBuilder = (func, output, mds) =>
                    {
                        // TODO: Function should be responsible for this. Refactor.
                        ColumnType[] funcCallTypes = ExtractCallTypes(func, mds);
                        var functor = AddFunctorOutputMappingHandler.MapToFunctor(funcCallTypes[0], funcCallTypes[1]);
                        Union2Type<MetadataColumn, Sql.value>[] fetchers = BuildFunctionArgumentFetchers(func, mds);

                        return (RowHolder inputRh, RowHolder outputRh) =>
                        {
                            functor.ExecCompute(
                                inputRh,
                                outputRh,
                                fetchers,
                                output
                            );
                        };
                    },
                }
            },
        };

        public static MetadataColumn GetMetadataInfoForOutput(Sql.columnSelect.Func func, MetadataColumn[] sourceInput)
        {
            string funcName = func.Item.Item1;

            if (FuncDictionary.TryGetValue(funcName, out MetadataOutputFunctorBuilderPair metadataOutFetcher))
            {
                return metadataOutFetcher.GetMetadataInfoForOutput(func, sourceInput);
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

        private static Union2Type<MetadataColumn, Sql.value>[] BuildFunctionArgumentFetchers(Sql.columnSelect.Func func, MetadataColumn[] sourceColumns)
        {
            Sql.scalarArgs args = func.Item.Item2;
            int numOfArgumnets = GetNumOfArguments(func);
            Union2Type<MetadataColumn, Sql.value>[] fetchers = new Union2Type<MetadataColumn, Sql.value>[numOfArgumnets];

            for (int argNum = 0; argNum < numOfArgumnets; argNum++)
            {
                Sql.value arg = GetArgNum(argNum, args);

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

            return fetchers;
        }

        public static Action<RowHolder, RowHolder> BuildFunctor(Sql.columnSelect.Func func, int outputPosition, MetadataColumn[] sourceColumns)
        {
            string funcName = func.Item.Item1;

            if (FuncDictionary.TryGetValue(funcName, out MetadataOutputFunctorBuilderPair metadataOutFetcher))
            {
                return metadataOutFetcher.FunctorBuilder(func, outputPosition, sourceColumns);
            }

            throw new InvalidFunctionNameException();
        }
    }
}
