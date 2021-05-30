using MetadataManager;
using PageManager;
using QueryProcessing.Exceptions;
using QueryProcessing.Utilities;
using System;

namespace QueryProcessing.Functions
{
    public class StringConcatOutputMappingHandler : IFunctionMappingHandler
    {
        public MetadataColumn GetMetadataInfoForOutput(Sql.valueOrFunc.FuncCall func, MetadataColumn[] metadataColumns)
        {
            ColumnType[] columnTypes = FuncCallMapper.ExtractCallTypes(func, metadataColumns);

            if (columnTypes.Length != 2)
            {
                throw new InvalidFunctionArgument("Add requires 2 arguments");
            }

            // TODO: What is the proper output length.
            // For now just hard code.
            const int MAX_STRING_FUNC_LENGTH_FOR_FUNC_RETURN = 256;
            return (columnTypes[0], columnTypes[1]) switch
            {
                (ColumnType.String, ColumnType.String) => new MetadataColumn(0, 0, "String_Concat_Result", new ColumnInfo(ColumnType.String, MAX_STRING_FUNC_LENGTH_FOR_FUNC_RETURN)),
                (ColumnType.StringPointer, ColumnType.String) => new MetadataColumn(0, 0, "String_Concat_Result", new ColumnInfo(ColumnType.String, MAX_STRING_FUNC_LENGTH_FOR_FUNC_RETURN)),
                (ColumnType.String, ColumnType.StringPointer) => new MetadataColumn(0, 0, "String_Concat_Result", new ColumnInfo(ColumnType.String, MAX_STRING_FUNC_LENGTH_FOR_FUNC_RETURN)),
                (ColumnType.StringPointer, ColumnType.StringPointer) => new MetadataColumn(0, 0, "String_Concat_Result", new ColumnInfo(ColumnType.String, MAX_STRING_FUNC_LENGTH_FOR_FUNC_RETURN)),
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
                // TODO: Deal with string pointers later.
                // I don't like having separate cases for string and string pointer at this level.
                (ColumnType.String, ColumnType.String) => new ConcatStrings(),
                _ => throw new InvalidFunctionArgument("Invalid type"),
            };
        }

        public class ConcatStrings : IFunctionCall
        {
            public void ExecCompute(RowHolder inputRowHolder, RowHolder outputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArguments, int outputPosition)
            {
                char[] argOneExtracted = sourceArguments[0].Match<char[]>(
                    (MetadataColumn md) => inputRowHolder.GetStringField(md.ColumnId),
                    (Sql.value val) => ((Sql.value.String)val).Item.ToCharArray());
                char[] argTwoExtracted = sourceArguments[1].Match<char[]>(
                    (MetadataColumn md) => inputRowHolder.GetStringField(md.ColumnId),
                    (Sql.value val) => ((Sql.value.String)val).Item.ToCharArray());

                string res = new string(argOneExtracted) + new string(argTwoExtracted);

                outputRowHolder.SetField(outputPosition, res.ToCharArray());
            }

            public IComparable ExecCompute(RowHolder inputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArguments)
            {
                char[] argOneExtracted = sourceArguments[0].Match<char[]>(
                    (MetadataColumn md) => inputRowHolder.GetStringField(md.ColumnId),
                    (Sql.value val) => ((Sql.value.String)val).Item.ToCharArray());
                char[] argTwoExtracted = sourceArguments[1].Match<char[]>(
                    (MetadataColumn md) => inputRowHolder.GetStringField(md.ColumnId),
                    (Sql.value val) => ((Sql.value.String)val).Item.ToCharArray());

                return new string(argOneExtracted) + new string(argTwoExtracted);
            }
        }
    }
}
