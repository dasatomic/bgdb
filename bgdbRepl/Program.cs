using bgdbRepl;
using CommandLine;
using ConsoleTables;
using DataStructures;
using ImageProcessing;
using PageManager;
using QueryProcessing;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using VideoProcessing;

namespace atomicdbstarter
{
    class Program
    {
        // TODO: Figure out usage.
        public class Options
        {
            [Option("set_load_path", Required = false)]
            public string TitanicSetPath { get; set; }

            [Option("rep_load_count", Required = false, Default = 1)]
            public int RepCount { get; set; }

            [Option("use_list_format", Required = false, Default = false)]
            public bool UseListFormat { get; set; }
        }

        static string GetValAsString(ColumnInfo ci, RowHolder row, int columnPosition)
        {
            string strVal = null;
            if (ci.ColumnType == ColumnType.Double)
            {
                double value = row.GetField<double>(columnPosition);
                strVal = string.Format("{0:0.###}", value);
            }
            else if (ci.ColumnType == ColumnType.Int)
            {
                int value = row.GetField<int>(columnPosition);
                strVal = value.ToString();
            }
            else if (ci.ColumnType == ColumnType.String)
            {
                strVal = new string(row.GetStringField(columnPosition));
            }

            return strVal;
        }

        static async Task PrintResultsFormatList(RowProvider rowProvider)
        {
            Console.WriteLine("---------------------");
            int totalCount = 0;
            await foreach (var row in rowProvider.Enumerator)
            {
                totalCount++;
                for (int i = 0; i < rowProvider.ColumnInfo.Length; i++)
                {
                    ColumnInfo columnInfo = rowProvider.ColumnInfo[i].ColumnType;
                    string lineToPrint = rowProvider.ColumnInfo[i].ColumnName + " -> " + GetValAsString(columnInfo, row, i);
                    Console.WriteLine(lineToPrint);
                }

                Console.WriteLine("---------------------");
            }
        }

        static async Task PrintResultsFormatTable(RowProvider rowProvider)
        {
            var table = new ConsoleTable(rowProvider.ColumnInfo.Select(ci => ci.ColumnName).ToArray());

            int totalCount = 0;
            await foreach (var row in rowProvider.Enumerator)
            {
                totalCount++;
                List<string> tableRow = new List<string>();

                for (int i = 0; i < rowProvider.ColumnInfo.Length; i++)
                {
                    ColumnInfo columnInfo = rowProvider.ColumnInfo[i].ColumnType;
                    string val = GetValAsString(columnInfo, row, i);
                    tableRow.Add(val);
                }

                table.AddRow(tableRow.ToArray());
            }

            if (table.Rows.Count > 0)
            {
                table.Write();
            }

            Console.WriteLine("----------------------");
            Console.WriteLine($"Total rows returned {totalCount}");
        }

        private static string GetTempFolderPath()
        {
            FileInfo dataRoot = new FileInfo(typeof(Program).Assembly.Location);
            string assemblyFolderPath = dataRoot.Directory.FullName;

            string path = Path.Combine(assemblyFolderPath, "temp");
            Directory.CreateDirectory(path);
            return path;
        }

        static async Task Main(string[] args)
        {
            string datasetPathToLoad = null;
            int repCount = 1;
            bool useListFormat = false;
            Parser.Default.ParseArguments<Options>(args).WithParsed<Options>(o =>
            {
                datasetPathToLoad = o.TitanicSetPath;
                repCount = o.RepCount;
                useListFormat = o.UseListFormat;
            });

            string fileName = "repl.db";
            const int fileSize = 1024 * 1024 * 100;

            var pageManager =  new PageManager.PageManager(4096, new FifoEvictionPolicy(200000, 5), new PersistedStream(fileSize, fileName, createNew: true)); ;

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"Booted Page Manager with file name {fileName}. File size {fileSize / (1024 * 1024)}MB. Creating db file from scratch.");

            var logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            Console.WriteLine($"Booted Log Manager. Using in memory stream.");

            StringHeapCollection stringHeap = null;

            await using (ITransaction tran = logManager.CreateTransaction(pageManager, "SETUP"))
            {
                stringHeap = new StringHeapCollection(pageManager, tran);
                await tran.Commit();
            }

            var metadataManager = new MetadataManager.MetadataManager(pageManager, stringHeap, pageManager, logManager);
            Console.WriteLine($"Booted Metadata Manager.");

            var videoChunker = new FfmpegVideoChunker(GetTempFolderPath(), new VideoProcessing.NoOpLogging());
            var videoProbe = new FfmpegProbeWrapper(new VideoProcessing.NoOpLogging());
            var videoChunkerCallback = SourceRegistration.VideoChunkerCallback(videoChunker, videoProbe);

            var videoToImage = new FfmpegFrameExtractor(GetTempFolderPath(), new VideoProcessing.NoOpLogging());
            var videoToImageCallback = SourceRegistration.VideoToImageCallback(videoToImage);
            AstToOpTreeBuilder treeBuilder = new AstToOpTreeBuilder(metadataManager, videoChunkerCallback, videoToImageCallback);

            var queryEntryGate = new QueryEntryGate(
                statementHandlers: new ISqlStatement[]
                {
                    new CreateTableStatement(metadataManager),
                    new InsertIntoTableStatement(treeBuilder),
                    new SelectStatement(treeBuilder),
                });

            IFunctionMappingHandler mappingHandler = new ImageObjectClassificationFuncMappingHandler();
            queryEntryGate.RegisterExternalFunction("CLASSIFY_IMAGE", mappingHandler);

            Console.WriteLine($"Query entry gate ready.");

            Console.WriteLine($"Transactions are implicit for now. Every command will be executed in separate transaction.");

            if (datasetPathToLoad != null)
            {
                Console.WriteLine("Loading dataset");
                Console.WriteLine("Duplicating dataset X{0} times", repCount);

                List<string> insertCommands = TitanicDatasetToSql.TitanicCsvToSql(datasetPathToLoad);
                int insertCount = 0;

                await using (ITransaction tran = logManager.CreateTransaction(pageManager))
                {
                    await queryEntryGate.Execute(insertCommands[0], tran).AllResultsAsync();
                    await tran.Commit();
                }

                for (int i  = 0; i < repCount; i++)
                {
                    foreach (string cmd in insertCommands.Skip(1))
                    {
                        await using (ITransaction tran = logManager.CreateTransaction(pageManager))
                        {
                            await queryEntryGate.Execute(cmd, tran).AllResultsAsync();
                            await tran.Commit();
                            insertCount++;
                        }
                    }

                    Console.WriteLine("Loaded iteration {0}/{1}", i + 1, repCount);
                }

                Console.WriteLine("Loaded {0} rows.", insertCount);
            }

            Console.WriteLine("====================");

            while (true)
            {
                try
                {
                    Console.ForegroundColor = ConsoleColor.White;
                    Console.Write(">");
                    string queryText = Console.ReadLine();

                    Stopwatch sw = Stopwatch.StartNew();

                    await using (ITransaction tran = logManager.CreateTransaction(pageManager))
                    {
                        RowProvider rowProvider = await queryEntryGate.BuildExecutionTree(queryText, tran);
                        Console.WriteLine();
                        Console.ForegroundColor = ConsoleColor.Blue;

                        if (useListFormat)
                        {
                            await PrintResultsFormatList(rowProvider);
                        }
                        else
                        {
                            await PrintResultsFormatTable(rowProvider);
                        }

                        Console.WriteLine($"Total running time {sw.Elapsed.TotalSeconds}s");
                        Console.WriteLine("Press any key to commit the transaction");
                        Console.ReadLine();

                        await tran.Commit();
                    }
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($" Error : {ex.GetType()}");
                    Console.WriteLine($" Message: {ex.Message}");
                    Console.WriteLine($" Callstack: {ex.StackTrace}");
                }
            }
        }
    }
}
