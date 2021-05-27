using MetadataManager;
using PageManager;
using QueryProcessing.Exceptions;
using QueryProcessing.Utilities;

namespace QueryProcessing.Functions
{
    static class FunctorArgChecks
    {
        public static void CheckInputArguments(Union2Type<MetadataColumn, Sql.value>[] sourceArguments, ColumnType[] acceptedColumnTypes)
        {
            if (sourceArguments.Length != acceptedColumnTypes.Length)
            {
                throw new InvalidFunctionArgument("Invalid number of arguments");
            }

            for (int i = 0; i < sourceArguments.Length; i++)
            {
                if (!sourceArguments[i].Match<bool>(
                    (md) => md.ColumnType.ColumnType == acceptedColumnTypes[i],
                    (val) => QueryProcessingAccessors.ValueToType(val) == acceptedColumnTypes[i]))
                {
                    throw new InvalidFunctionArgument($"This type is not accepted by this function.");
                }
            }
        }
    }
}
