using PageManager;
using System.Collections.Generic;

namespace QueryProcessing
{
    public interface ISqlStatement
    {
        IAsyncEnumerable<RowHolder> Execute(Sql.DmlDdlSqlStatement statement, ITransaction tran);

        bool ShouldExecute(Sql.DmlDdlSqlStatement statement);
    }
}
