using MetadataManager;
using PageManager;
using System;
using QueryProcessing.Utilities;
using QueryProcessing.Exceptions;
using System.Collections.Generic;
using QueryProcessing.Functions;

namespace QueryProcessing
{
    /// <summary>
    /// Responsible for mapping function calls from syntax tree to functor class.
    /// </summary>
    public static class FuncCallMapper
    {
        private struct MetadataOutputFunctorBuilderPair
        {
            public Func<Sql.valueOrFunc.FuncCall, MetadataColumn[] /* source columns */, MetadataColumn /* ret - output type */> GetMetadataInfoForOutput;
            public Func<Sql.valueOrFunc.FuncCall, int /* output position */, MetadataColumn[] /* source columns */, Action<RowHolder, RowHolder> /* ret - Action mapper */> FunctorBuilder;

            public Func<Sql.valueOrFunc.FuncCall, MetadataColumn[] /* source columns */, Func<RowHolder, IComparable>> FunctionReturnValueBuilder;
        }

        // TODO: Mapping handlers should be singletons.
        private static AddFunctorOutputMappingHandler addMappingHandler = new AddFunctorOutputMappingHandler();
        private static StringConcatOutputMappingHandler concatMappingHandler = new StringConcatOutputMappingHandler();

        /// <summary>
        /// Returns Action that maps input rowholder to output rowholder with func applied.
        /// </summary>
        private static Action<RowHolder, RowHolder> FunctionBuilder(Sql.valueOrFunc.FuncCall func, int output, MetadataColumn[] sourceColumns, IFunctionMappingHandler mappingHandler)
        {
            ColumnType[] funcCallTypes = ExtractCallTypes(func, sourceColumns);
            var functor = mappingHandler.MapToFunctor(funcCallTypes);
            Union2Type<MetadataColumn, Sql.value>[] fetchers = BuildFunctionArgumentFetchers(func, sourceColumns);

            return (RowHolder inputRh, RowHolder outputRh) =>
            {
                functor.ExecCompute(
                    inputRh,
                    outputRh,
                    fetchers,
                    output
                );
            };
        }

        /// <summary>
        /// Returns func that extracts IComparable.
        /// </summary>
        /// <param name="func"></param>
        /// <param name="sourceColumns"></param>
        /// <param name="mappingHandler"></param>
        /// <returns></returns>
        private static Func<RowHolder, IComparable> FunctionResultBuilder(Sql.valueOrFunc.FuncCall func, MetadataColumn[] sourceColumns, IFunctionMappingHandler mappingHandler)
        {
            ColumnType[] funcCallTypes = ExtractCallTypes(func, sourceColumns);
            var functor = mappingHandler.MapToFunctor(funcCallTypes);
            Union2Type<MetadataColumn, Sql.value>[] fetchers = BuildFunctionArgumentFetchers(func, sourceColumns);

            return (RowHolder inputRh) =>
            {
                return functor.ExecCompute(
                    inputRh,
                    fetchers
                );
            };
        }

        private static Dictionary<string, MetadataOutputFunctorBuilderPair> FuncDictionary = new Dictionary<string, MetadataOutputFunctorBuilderPair>()
        {
            {
                "ADD", new MetadataOutputFunctorBuilderPair()
                {
                    GetMetadataInfoForOutput = (func, mds) => addMappingHandler.GetMetadataInfoForOutput(func, mds),
                    FunctorBuilder = (func, output, mds) => FunctionBuilder(func, output, mds, addMappingHandler),
                    FunctionReturnValueBuilder = (func, mds) => FunctionResultBuilder(func, mds, addMappingHandler),
                }
            },
            {
                "CONCAT", new MetadataOutputFunctorBuilderPair()
                {
                    GetMetadataInfoForOutput = (func, mds) => concatMappingHandler.GetMetadataInfoForOutput(func, mds),
                    FunctorBuilder = (func, output, mds) => FunctionBuilder(func, output, mds, concatMappingHandler),
                    FunctionReturnValueBuilder = (func, mds) => FunctionResultBuilder(func, mds, concatMappingHandler),
                }
            }
        };

        /// <summary>
        /// Externally exposed handler for func registration.
        /// Use it as part of engine boot to register funcs from external components.
        /// </summary>
        public static void RegisterFunc(string functionName, IFunctionMappingHandler mappingHandler)
        {
            FuncDictionary.Add(functionName, new MetadataOutputFunctorBuilderPair()
            {
                GetMetadataInfoForOutput = (func, mds) => mappingHandler.GetMetadataInfoForOutput(func, mds),
                FunctorBuilder = (func, output, mds) => FunctionBuilder(func, output, mds, mappingHandler),
                FunctionReturnValueBuilder = (func, mds) => FunctionResultBuilder(func, mds, mappingHandler),
            });
        }

        public static MetadataColumn GetMetadataInfoForOutput(Sql.valueOrFunc.FuncCall func, MetadataColumn[] sourceInput)
        {
            string funcName = func.Item.Item1;

            if (FuncDictionary.TryGetValue(funcName, out MetadataOutputFunctorBuilderPair metadataOutFetcher))
            {
                return metadataOutFetcher.GetMetadataInfoForOutput(func, sourceInput);
            }

            throw new InvalidFunctionNameException();
        }

        private static int GetNumOfArguments(Sql.valueOrFunc.FuncCall func)
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

        public static ColumnType[] ExtractCallTypes(Sql.valueOrFunc.FuncCall func, MetadataColumn[] metadataColumns)
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

        private static Union2Type<MetadataColumn, Sql.value>[] BuildFunctionArgumentFetchers(Sql.valueOrFunc.FuncCall func, MetadataColumn[] sourceColumns)
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

        public static Action<RowHolder, RowHolder> BuildRowHolderMapperFunctor(Sql.valueOrFunc.FuncCall func, int outputPosition, MetadataColumn[] sourceColumns)
        {
            string funcName = func.Item.Item1;

            if (FuncDictionary.TryGetValue(funcName, out MetadataOutputFunctorBuilderPair metadataOutFetcher))
            {
                return metadataOutFetcher.FunctorBuilder(func, outputPosition, sourceColumns);
            }

            throw new InvalidFunctionNameException();
        }

        public static Func<RowHolder, IComparable> BuildResultFunctor(Sql.valueOrFunc.FuncCall func, MetadataColumn[] sourceColumns)
        {
            string funcName = func.Item.Item1;

            if (FuncDictionary.TryGetValue(funcName, out MetadataOutputFunctorBuilderPair metadataOutFetcher))
            {
                return metadataOutFetcher.FunctionReturnValueBuilder(func, sourceColumns);
            }

            throw new InvalidFunctionNameException();
        }
    }
}
