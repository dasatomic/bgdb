using Microsoft.ML;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace ImageProcessing
{
    public class TFModelImageLabelScorer
    {
        private readonly string modelLocation;
        private readonly string labelsLocation;
        private readonly MLContext mlContext;

        private static string ImageReal = nameof(ImageReal);

        private readonly PredictionEngine<ImageDataSource, ImageLabelPrediction> model;

        private static string GetAssetsPath()
        {
            FileInfo dataRoot = new FileInfo(typeof(TFModelImageLabelScorer).Assembly.Location);
            string assemblyFolderPath = dataRoot.Directory.FullName;
            return Path.Combine(assemblyFolderPath, "assets");
        }

        public TFModelImageLabelScorer(string modelFileName, string labelsFileName)
        {
            string assetsPath = GetAssetsPath();
            this.modelLocation = Path.Combine(assetsPath, modelFileName);
            this.labelsLocation = Path.Combine(assetsPath, labelsFileName);
            this.mlContext = new MLContext();
            this.model = LoadModel(modelLocation);

        }

        public struct ImageNetSettings
        {
            public const int imageHeight = 224;
            public const int imageWidth = 224;
            public const float mean = 117;
            public const bool channelsLast = true;
            public const int returnTopNLabels = 10;
            public const float probabilityThreshold = 0.1f;
        }

        public struct InceptionSettings
        {
            // input tensor name
            public const string inputTensorName = "input";

            // output tensor name
            public const string outputTensorName = "softmax2";
        }

        public ImageLabelPredictionProbability ScoreSingle(string path)
        {
            return PredictDataUsingModelSinge(labelsLocation, path);
        }

        private IEnumerable<ImageDataSource> LoadSource()
        {
            return Enumerable.Empty<ImageDataSource>();
        }

        private PredictionEngine<ImageDataSource, ImageLabelPrediction> LoadModel(string modelLocation)
        {
            // Don't train anything.
            // Keep the source empty and feed with when scorer is invoked.
            // TODO: This is ugly. Not sure if there is a nicer way to do this.
            var data = mlContext.Data.LoadFromEnumerable(LoadSource());

            var pipeline = mlContext.Transforms.LoadImages(outputColumnName: "input", imageFolder: ".", inputColumnName: nameof(ImageDataSource.ImagePath))
                            .Append(mlContext.Transforms.ResizeImages(outputColumnName: "input", imageWidth: ImageNetSettings.imageWidth, imageHeight: ImageNetSettings.imageHeight, inputColumnName: "input"))
                            .Append(mlContext.Transforms.ExtractPixels(outputColumnName: "input", interleavePixelColors: ImageNetSettings.channelsLast, offsetImage: ImageNetSettings.mean))
                            .Append(mlContext.Model.LoadTensorFlowModel(modelLocation).
                            ScoreTensorFlowModel(outputColumnNames: new[] { "softmax2" },
                                                inputColumnNames: new[] { "input" }, addBatchDimensionInput: true));

            ITransformer model = pipeline.Fit(data);

            var predictionEngine = mlContext.Model.CreatePredictionEngine<ImageDataSource, ImageLabelPrediction>(model);

            return predictionEngine;
        }

        protected ImageLabelPredictionProbability PredictDataUsingModelSinge(string labelsLocation, string path)
        {
            string[] labels = File.ReadAllLines(labelsLocation);

            ImageDataSource dataSource = new ImageDataSource() { ImagePath = path };

                var probs = this.model.Predict(dataSource).PredictedLabels;
                var bestLabels = GetBestLabels(labels, probs, ImageNetSettings.returnTopNLabels);

                return new ImageLabelPredictionProbability()
                {
                    ImagePath = dataSource.ImagePath,
                    PredictedLabels = bestLabels.Item1,
                    Probabilities = bestLabels.Item2,
                };
        }

        private static (string[], float[]) GetBestLabels(string[] labels, float[] probs, int topN)
        {
            // TODO: This is naive slow implementation.
            List<string> bestLabels = new List<string>();
            List<float> bestProbabilities = new List<float>();
            var lblsList = labels.ToList();
            var probsList = probs.ToList();

            for (int i = 0; i < topN; i++)
            {
                var max = probsList.Max();
                var index = probsList.IndexOf(max);

                if (max >= ImageNetSettings.probabilityThreshold)
                {
                    bestLabels.Add(lblsList[index]);
                    bestProbabilities.Add(max);
                }

                lblsList.RemoveAt(index);
                probsList.RemoveAt(index);
            }

            return (bestLabels.ToArray(), bestProbabilities.ToArray());
        }
    }
}
