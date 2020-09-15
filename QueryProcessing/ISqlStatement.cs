using PageManager;
using System.Collections.Generic;

namespace QueryProcessing
{
    public interface ISqlStatement
    {
        IAsyncEnumerable<RowHolderFixed> Execute(Sql.DmlDdlSqlStatement statement, ITransaction tran);

        bool ShouldExecute(Sql.DmlDdlSqlStatement statement);
    }
}
