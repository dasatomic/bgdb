using bgdbRepl;
using CommandLine;
using DataStructures;
using PageManager;
using QueryProcessing;
using System;
using System.Collections.Generic;
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

            [Option("rep_load_count", Required = false, Default = 1)]
            public int RepCount { get; set; }
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

            int missingWhiteSpace = Math.Max(tableWidth - strVal.Length, 0);
            char[] ws =Enumerable.Repeat(' ', missingWhiteSpace).ToArray();
            return (new string(ws)) + strVal;
        }

        static async Task Main(string[] args)
        {
            string datasetPathToLoad = null;
            int repCount = 1;
            Parser.Default.ParseArguments<Options>(args).WithParsed<Options>(o =>
            {
                datasetPathToLoad = o.TitanicSetPath;
                repCount = o.RepCount;
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
                    Console.WriteLine($" Message: {ex.Message}");
                    Console.WriteLine($" Callstack: {ex.StackTrace}");
                }
            }
        }
    }
}
