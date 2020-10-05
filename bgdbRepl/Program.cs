using bgdbRepl;
using CommandLine;
using DataStructures;
using PageManager;
using QueryProcessing;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace atomicdbstarter
{
    class Program
    {
        // TODO: Figure out usage.
        public class Options
        {
            [Option("set_load_path", Required = false)]
            public string TitanicSetPath { get; set; }
        }

        static int GetColumnWidth(ColumnInfo ci)
        {
            if (ci.ColumnType == ColumnType.Double)
            {
                return 10;
            }
            else if (ci.ColumnType == ColumnType.Int)
            {
                return 10;
            }
            else if (ci.ColumnType == ColumnType.String)
            {
                return ci.RepCount + 2;
            }
            else
            {
                throw new ArgumentException();
            }
        }

        static string GetColumnString(ColumnInfo ci, int tableWidth, RowHolder row, int columnPosition)
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

            int missingWhiteSpace = tableWidth - strVal.Length;
            char[] ws =Enumerable.Repeat(' ', missingWhiteSpace).ToArray();
            return (new string(ws)) + strVal;
        }

        static async Task Main(string[] args)
        {
            string datasetPathToLoad = null;
            Parser.Default.ParseArguments<Options>(args).WithParsed<Options>(o =>
            {
                datasetPathToLoad = o.TitanicSetPath;
            });

            string fileName = "repl.db";
            const int fileSize = 1024 * 1024 * 10;

            var pageManager =  new PageManager.PageManager(4096, new FifoEvictionPolicy(1000, 5), new PersistedStream(fileSize, fileName, createNew: true)); ;

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

            AstToOpTreeBuilder treeBuilder = new AstToOpTreeBuilder(metadataManager);

            var queryEntryGate = new QueryEntryGate(
                statementHandlers: new ISqlStatement[]
                {
                    new CreateTableStatement(metadataManager),
                    new InsertIntoTableStatement(treeBuilder),
                    new SelectStatement(treeBuilder),
                });

            Console.WriteLine($"Query entry gate ready.");

            Console.WriteLine($"Transactions are implicit for now. Every command will be executed in separate transaction.");

            if (datasetPathToLoad != null)
            {
                Console.WriteLine("Loading dataset");
                foreach (string cmd in TitanicDatasetToSql.TitanicCsvToSql(datasetPathToLoad))
                {
                    await using (ITransaction tran = logManager.CreateTransaction(pageManager))
                    {
                        try
                        {
                            await queryEntryGate.Execute(cmd, tran).AllResultsAsync();
                            await tran.Commit();
                        }
                        catch (Exception)
                        {
                            await tran.Rollback();
                        }
                    }
                }
            }

            Console.WriteLine("====================");

            while (true)
            {
                try
                {
                    Console.ForegroundColor = ConsoleColor.White;
                    Console.Write(">");
                    string queryText = Console.ReadLine();

                    await using (ITransaction tran = logManager.CreateTransaction(pageManager))
                    {
                        RowProvider rowProvider = await queryEntryGate.BuildExecutionTree(queryText, tran);
                        Console.WriteLine();
                        Console.ForegroundColor = ConsoleColor.Blue;

                        int totalWidth = 0;
                        foreach (var ci in rowProvider.ColumnInfo)
                        {
                            int width = GetColumnWidth(ci.ColumnType);

                            int whitespaceCount = width - ci.ColumnName.Length;
                            totalWidth += width + whitespaceCount + 2;
                            Console.Write("|");

                            for (int i = 0; i < whitespaceCount; i++)
                            {
                                Console.Write(" ");
                            }

                            Console.Write(ci.ColumnName);
                            Console.Write(" ");
                        }

                        Console.Write("|");

                        Console.WriteLine();
                        Console.WriteLine(new string(Enumerable.Repeat('-', totalWidth).ToArray()));

                        int totalCount = 0;
                        await foreach (var row in rowProvider.Enumerator)
                        {
                            totalCount++;

                            int columnPos = 0;
                            for (int i = 0; i < rowProvider.ColumnInfo.Length; i++)
                            {
                                ColumnInfo columnInfo = rowProvider.ColumnInfo[i].ColumnType;
                                int width = GetColumnWidth(columnInfo);
                                string valToPrint = GetColumnString(columnInfo, width, row, columnPos);

                                Console.Write("|");
                                Console.Write(valToPrint);
                                Console.Write(" ");

                                columnPos++;
                            }

                            Console.Write("|");

                            Console.WriteLine();
                            Console.WriteLine(new string(Enumerable.Repeat('-', totalWidth).ToArray()));
                        }

                        Console.WriteLine();
                        Console.WriteLine(new string(Enumerable.Repeat('-', totalWidth).ToArray()));

                        Console.WriteLine($"Total rows returned {totalCount}");

                        await tran.Commit();
                    }
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($" Error : {ex.GetType()}");
                }
            }
        }
    }
}
