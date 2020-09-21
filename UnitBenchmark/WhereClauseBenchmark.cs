using BenchmarkDotNet.Attributes;
using PageManager;
using QueryProcessing;
using System.Linq;
using System.Threading.Tasks;

namespace UnitBenchmark
{
    [RPlotExporter]
    public class WhereClauseBenchmark
    {
        [Params(10000, 20000, 40000, 80000, 100000)]
        public int RowsInTableNumber;

        private QueryEntryGate queryEntryGate;
        private LogManager.ILogManager logManager;
        private IPageManager pageManager;

        [GlobalSetup]
        public async Task Setup()
        {
            (logManager, pageManager, queryEntryGate) = await BenchmarkUtils.GetLogAndQueryEntryGate();
            await using (ITransaction tran = logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE Table (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(10) c)";
                await queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            for (int i = 0; i < this.RowsInTableNumber; i++)
            {
                await using (ITransaction tran = logManager.CreateTransaction(pageManager, "INSERT"))
                {
                    string insertQuery = $"INSERT INTO Table VALUES ({i}, {i + 0.1}, '{i}')";
                    await queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                    await tran.Commit();
                }
            }
        }

        [Benchmark]
        public async Task InsertIntoTable()
        {
            await using (ITransaction tran = logManager.CreateTransaction(pageManager, "FILTER"))
            {
                string filterQuery = $"SELECT a, b, c FROM Table WHERE a < 10 AND b > 10.0";
                await queryEntryGate.Execute(filterQuery, tran).ToArrayAsync();
                await tran.Commit();
            }
        }
    }
}
