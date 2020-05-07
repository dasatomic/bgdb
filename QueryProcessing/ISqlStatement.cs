using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public interface ISqlStatement
    {
        Task<IEnumerable<Row>> Execute(Sql.DmlDdlSqlStatement statement);

        bool ShouldExecute(Sql.DmlDdlSqlStatement statement);
    }
}
