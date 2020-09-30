using PageManager;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public interface ISqlStatement
    {
        Task<RowProvider> BuildTree(Sql.DmlDdlSqlStatement statement, ITransaction tran);

        bool ShouldExecute(Sql.DmlDdlSqlStatement statement);
    }
}
