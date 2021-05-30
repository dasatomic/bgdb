using MetadataManager;
using PageManager;
using QueryProcessing.Utilities;

/// <summary>
/// TODO: This should be auto generated file that covers
/// type => arg matrix.
/// </summary>
namespace QueryProcessing.Functions
{
    public abstract class FunctionArg1Extractor<A>
    {
        public A ArgOne;
    }

    public abstract class FunctionArg2Extractor<A, B>
    {
        public A ArgOne;
        public B ArgTwo;
    }

    public abstract class FunctionArg2Extractor<A, B, C>
    {
        public A ArgOne;
        public B ArgTwo;
        public C ArgThree;
    }

    public class FunctorArgExtractIntInt : FunctionArg2Extractor<int, int>
    {
        public FunctorArgExtractIntInt(RowHolder inputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArgs)
        {
            this.ArgOne = sourceArgs[0].Match<int>(
                (MetadataColumn md) => inputRowHolder.GetField<int>(md.ColumnId),
                (Sql.value val) => ((Sql.value.Int)val).Item);
            this.ArgTwo = sourceArgs[1].Match<int>(
                (MetadataColumn md) => inputRowHolder.GetField<int>(md.ColumnId),
                (Sql.value val) => ((Sql.value.Int)val).Item);
        }
    }

    public class FunctorArgExtractIntDouble : FunctionArg2Extractor<int, double>
    {
        public FunctorArgExtractIntDouble(RowHolder inputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArgs)
        {
            this.ArgOne = sourceArgs[0].Match<int>(
                (MetadataColumn md) => inputRowHolder.GetField<int>(md.ColumnId),
                (Sql.value val) => ((Sql.value.Int)val).Item);
            this.ArgTwo = sourceArgs[1].Match<double>(
                (MetadataColumn md) => inputRowHolder.GetField<double>(md.ColumnId),
                (Sql.value val) => ((Sql.value.Int)val).Item);
        }
    }

    public class FunctorArgExtractDoubleInt : FunctionArg2Extractor<double, int>
    {
        public FunctorArgExtractDoubleInt(RowHolder inputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArgs)
        {
            this.ArgOne = sourceArgs[0].Match<double>(
                (MetadataColumn md) => inputRowHolder.GetField<double>(md.ColumnId),
                (Sql.value val) => ((Sql.value.Int)val).Item);
            this.ArgTwo = sourceArgs[1].Match<int>(
                (MetadataColumn md) => inputRowHolder.GetField<int>(md.ColumnId),
                (Sql.value val) => ((Sql.value.Int)val).Item);
        }
    }

    public class FunctorArgExtractDoubleDouble : FunctionArg2Extractor<double, double>
    {
        public FunctorArgExtractDoubleDouble(RowHolder inputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArgs)
        {
            this.ArgOne = sourceArgs[0].Match<double>(
                (MetadataColumn md) => inputRowHolder.GetField<double>(md.ColumnId),
                (Sql.value val) => ((Sql.value.Int)val).Item);
            this.ArgTwo = sourceArgs[1].Match<double>(
                (MetadataColumn md) => inputRowHolder.GetField<double>(md.ColumnId),
                (Sql.value val) => ((Sql.value.Int)val).Item);
        }
    }
}
