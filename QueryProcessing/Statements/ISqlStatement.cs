using PageManager;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public interface ISqlStatement
    {
        Task<RowProvider> BuildTree(Sql.DmlDdlSqlStatement statement, ITransaction tran, InputStringNormalizer stringNormalizer);

        bool ShouldExecute(Sql.DmlDdlSqlStatement statement);
    }
}
