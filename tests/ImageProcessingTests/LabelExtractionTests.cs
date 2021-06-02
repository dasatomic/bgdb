using ImageProcessing;
using System.IO;
using System.Collections.Generic;
using NUnit.Framework;

namespace ImageProcessingTests
{
    public class LabelExtractionTests
    {
        private static string GetImageInputPath()
        {
            FileInfo dataRoot = new FileInfo(typeof(LabelExtractionTests).Assembly.Location);
            string assemblyFolderPath = dataRoot.Directory.FullName;
            return Path.Combine(assemblyFolderPath, "assets", "images");
        }

        [Test]
        public void ExtractLabelSingle()
        {
            var imagesFolder = GetImageInputPath();
            var inceptionPb = "tensorflow_inception_graph.pb";
            var labelsTxt = "imagenet_comp_graph_label_strings.txt";

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

            TFModelImageLabelScorer scorer = new TFModelImageLabelScorer(inceptionPb, labelsTxt);
            foreach (var file in Directory.GetFiles(imagesFolder))
            {
                ImageLabelPredictionProbability score = scorer.ScoreSingle(file);

                string fileName = Path.GetFileName(score.ImagePath);
                string correctLabel = mappings[fileName];
                Assert.AreEqual(correctLabel, score.PredictedLabels[0]);
            }
        }
    }
}
