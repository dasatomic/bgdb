using BenchmarkDotNet.Attributes;
using LogManager;
using PageManager;
using QueryProcessing;
using System.Linq;
using System.Threading.Tasks;

namespace UnitBenchmark
{

    [RPlotExporter]
    public class CreateTableBenchmark
    {
        [Params(100, 1000)]
        public int TableNumber;

        [Benchmark]
        public async Task CreateTable()
        {
            (ILogManager logManager, IPageManager pageManager, QueryEntryGate queryEntryGate) = await BenchmarkUtils.GetLogAndQueryEntryGate();

            for (int i = 0; i < this.TableNumber; i++)
            {
                await using (ITransaction tran = logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
                {
                    string createTableQuery = $"CREATE TABLE Table{i} (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(10) c)";
                    await queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                    await tran.Commit();
                }
            }
        }
    }
}
