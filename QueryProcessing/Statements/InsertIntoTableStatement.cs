using MetadataManager;
using PageManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Headers;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class InsertIntoTableStatement : ISqlStatement
    {
        private AstToOpTreeBuilder treeBuilder;

        public InsertIntoTableStatement(AstToOpTreeBuilder treeBuilder)
        {
            this.treeBuilder = treeBuilder;
        }

        public async Task<RowProvider> BuildTree(Sql.DmlDdlSqlStatement statement, ITransaction tran, InputStringNormalizer stringNormalizer)
        {
            if (!statement.IsInsert)
            {
                throw new ArgumentException();
            }

            Sql.DmlDdlSqlStatement.Insert insertStatement = ((Sql.DmlDdlSqlStatement.Insert)statement);
            IPhysicalOperator<RowHolder> rootOp = await this.treeBuilder.ParseInsertStatement(insertStatement.Item, tran, stringNormalizer).ConfigureAwait(false);

            return new RowProvider(rootOp.Iterate(tran), new MetadataColumn[0]);
        }

        public bool ShouldExecute(Sql.DmlDdlSqlStatement statement) => statement.IsInsert;
    }
}
