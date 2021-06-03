using Microsoft.ML.Data;

namespace ImageProcessing
{
    public class ImageDataSource
    {
        [LoadColumn(0)]
        public string ImagePath;
    }

    public class ImageLabelPredictionProbability : ImageDataSource
    {
        public string[] PredictedLabels;
        public float[] Probabilities { get; set; }
    }

    public class ImageLabelPrediction
    {
        [ColumnName(TFModelImageLabelScorer.InceptionSettings.outputTensorName)]
        public float[] PredictedLabels;
    }
}
