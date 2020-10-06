using FSharp.Text.Lexing;
using Microsoft.FSharp.Core;
using PageManager;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class QueryEntryGate
    {
        private IEnumerable<ISqlStatement> statementHandlers;

        public QueryEntryGate(IEnumerable<ISqlStatement> statementHandlers)
        {
            this.statementHandlers = statementHandlers;
        }

        private Sql.DmlDdlSqlStatement BuildStatement(string query, InputStringNormalizer stringNormalizer)
        {
            query = stringNormalizer.InputForLexer;

            var lexbuf = LexBuffer<char>.FromString(query);
            Func<LexBuffer<char>, SqlParser.token> func = (x) => SqlLexer.tokenize(x);
            return SqlParser.startCT(FuncConvert.FromFunc(func), lexbuf);
        }

        public async IAsyncEnumerable<RowHolder> Execute(string queryText, ITransaction tran)
        {
            RowProvider provider = await this.BuildExecutionTree(queryText, tran);

            await foreach (RowHolder row in provider.Enumerator)
            {
                yield return row;
            }
        }

        public async Task<RowProvider> BuildExecutionTree(string queryText, ITransaction tran)
        {
            InputStringNormalizer stringNormalizer = new InputStringNormalizer(queryText);
            Sql.DmlDdlSqlStatement statement = BuildStatement(queryText, stringNormalizer);

            foreach (ISqlStatement handler in statementHandlers)
            {
                if (handler.ShouldExecute(statement))
                {
                    return await handler.BuildTree(statement, tran, stringNormalizer);
                }
            }

            throw new ArgumentException();
        }
    }
}
