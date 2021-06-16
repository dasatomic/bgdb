using System;
using MetadataManager;
using PageManager;
using QueryProcessing;
using QueryProcessing.Exceptions;
using QueryProcessing.Functions;
using QueryProcessing.Utilities;

namespace ImageProcessing
{
    public class ImageObjectClassificationFuncMappingHandler : IFunctionMappingHandler
    {
        public MetadataColumn GetMetadataInfoForOutput(Sql.valueOrFunc.FuncCall func, MetadataColumn[] metadataColumns)
        {
            const int MaxReturnTypeLength = 256;
            ColumnType[] columnTypes = FuncCallMapper.ExtractCallTypes(func, metadataColumns);

            if (columnTypes.Length != 1)
            {
                throw new InvalidFunctionArgument("Object classification requires 1 argument (image path)");
            }

            if (columnTypes[0] != ColumnType.String && columnTypes[0] != ColumnType.StringPointer)
            {
                throw new InvalidFunctionArgument("invalid argument type for object classification");
            }

            return new MetadataColumn(0, 0, "Object_Classification_Result", new ColumnInfo(ColumnType.String, MaxReturnTypeLength));
        }

        public IFunctionCall MapToFunctor(ColumnType[] args)
        {
            if (args.Length != 1)
            {
                throw new InvalidFunctionArgument("Object classification requires 1 argument (image path)");
            }

            if (args[0] != ColumnType.String && args[0] != ColumnType.StringPointer)
            {
                throw new InvalidFunctionArgument("invalid argument type for object classification");
            }

            return new ImageObjectClassificationFunctor();
        }
    }

    public class ImageObjectClassificationFunctor : IFunctionCall
    {
        public void ExecCompute(RowHolder inputRowHolder, RowHolder outputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArguments, int outputPosition)
        {
            FunctorArgChecks.CheckInputArguments(sourceArguments, new[] { ColumnType.String });
            FunctorArgExtractString arg = new FunctorArgExtractString(inputRowHolder, sourceArguments);
            const string inceptionPb = "tensorflow_inception_graph.pb";
            const string labelsTxt = "imagenet_comp_graph_label_strings.txt";

            TFModelImageLabelScorer scorer = new TFModelImageLabelScorer(inceptionPb, labelsTxt);
            ImageLabelPredictionProbability score = scorer.ScoreSingle(arg.ArgOne);

            if (score.PredictedLabels.Length > 0)
            {
                outputRowHolder.SetField(outputPosition, score.PredictedLabels[0].ToCharArray());
            }
            else
            {
                outputRowHolder.SetField(outputPosition, "unknown".ToCharArray());
            }
        }

        public IComparable ExecCompute(RowHolder inputRowHolder, Union2Type<MetadataColumn, Sql.value>[] sourceArguments)
        {
            FunctorArgChecks.CheckInputArguments(sourceArguments, new[] { ColumnType.String });
            FunctorArgExtractString arg = new FunctorArgExtractString(inputRowHolder, sourceArguments);
            throw new NotImplementedException();
        }
    }
}
