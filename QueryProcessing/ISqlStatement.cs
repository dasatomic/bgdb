using PageManager;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public interface ISqlStatement
    {
        IAsyncEnumerable<Row> Execute(Sql.DmlDdlSqlStatement statement, ITransaction tran);

        bool ShouldExecute(Sql.DmlDdlSqlStatement statement);
    }
}
