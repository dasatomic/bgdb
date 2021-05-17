using ImageProcessing;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using NUnit.Framework;

namespace ImageProcessingTests
{
    public class LabelExtractionTests
    {
        private static string GetImageInputPath()
        {
            const string assetsRelativePath = @"assets\images";
            FileInfo dataRoot = new FileInfo(typeof(LabelExtractionTests).Assembly.Location);
            string assemblyFolderPath = dataRoot.Directory.FullName;
            string fullPath = Path.Combine(assemblyFolderPath, assetsRelativePath);
            return fullPath;
        }

        [Test]
        public void ExtractLabelsE2EWithImageNetModel()
        {
            var imagesFolder = GetImageInputPath();
            var inceptionPb = "tensorflow_inception_graph.pb";
            var labelsTxt = "imagenet_comp_graph_label_strings.txt";

            TFModelImageLabelScorer scorer = new TFModelImageLabelScorer(imagesFolder, inceptionPb, labelsTxt);
            ImageLabelPredictionProbability[] scores = scorer.Score().ToArray();

            // TODO: These are results of imagenet model.
            // Hardcoding results.
            // With support for different models this will need to be more generic.
            Dictionary<string, string> mappings = new Dictionary<string, string>()
            {
                { "broccoli.jpg", "broccoli" },
                { "canoe3.jpg", "canoe" },
                { "coffeepot2.jpg", "coffeepot" },
                { "nba.jfif", "basketball" },
                { "office.jpg", "desktop computer" },
                { "yoda.jfif", "trench coat" }
            };

            foreach (var label in scores)
            {
                string fileName = Path.GetFileName(label.ImagePath);
                string correctLabel = mappings[fileName];

                Assert.AreEqual(correctLabel, label.PredictedLabels[0]);
            }
        }
    }
}
