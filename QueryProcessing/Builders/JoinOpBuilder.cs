using MetadataManager;
using Microsoft.FSharp.Core;
using PageManager;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace QueryProcessing
{
    class JoinOpBuilder : IStatementTreeBuilder
    {
        private readonly MetadataManager.MetadataManager metadataManager;

        public JoinOpBuilder(MetadataManager.MetadataManager metadataManager)
        {
            this.metadataManager = metadataManager;
        }

        public async Task<IPhysicalOperator<RowHolder>> BuildStatement(Sql.sqlStatement statement, ITransaction tran, IPhysicalOperator<RowHolder> source, InputStringNormalizer stringNormalizer)
        {
            if (statement.Joins.Any())
            {
                IPhysicalOperator<RowHolder> currJoinSource = source;
                for (int i = 0; i < statement.Joins.Length; i++)
                {
                    if (!statement.Joins[i].Item2.IsInner)
                    {
                        throw new NotSupportedException("Only inner join is supported at this point.");
                    }

                    MetadataTable joinRightTable = await this.metadataManager.GetTableManager().GetByName(statement.Joins[i].Item1, tran).ConfigureAwait(false);
                    PhyOpScan scanOpRight = new PhyOpScan(joinRightTable.Collection, tran, joinRightTable.Columns, joinRightTable.TableName);

                    Func<RowHolder, bool> filter = (_) => true;
                    if (FSharpOption<Sql.where>.get_IsSome(statement.Joins[i].Item3))
                    {
                        filter = FilterStatementBuilder.EvalWhere(
                            statement.Joins[i].Item3.Value,
                            QueryProcessingAccessors.MergeColumns(currJoinSource.GetOutputColumns(), scanOpRight.GetOutputColumns()),
                            stringNormalizer);
                    }

                    currJoinSource = new PhyOpLoopInnerJoin(currJoinSource, scanOpRight, filter);
                }

                return currJoinSource;
            }
            else
            {
                return source;
            }
        }
    }
}
