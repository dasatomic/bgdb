using PageManager;
using System.Threading.Tasks;

namespace QueryProcessing
{
    interface IStatementTreeBuilder
    {
        Task<IPhysicalOperator<RowHolder>> BuildStatement(Sql.sqlStatement statement, ITransaction tran, IPhysicalOperator<RowHolder> source, InputStringNormalizer inputStringNormalizer);
    }
}
