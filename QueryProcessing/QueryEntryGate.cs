using FSharp.Text.Lexing;
using Microsoft.FSharp.Core;
using PageManager;
using System;
using System.Collections.Generic;

namespace QueryProcessing
{
    public class QueryEntryGate
    {
        private IEnumerable<ISqlStatement> statementHandlers;

        public QueryEntryGate(IEnumerable<ISqlStatement> statementHandlers)
        {
            this.statementHandlers = statementHandlers;
        }

        private Sql.DmlDdlSqlStatement BuildStatement(string query)
        {
            var lexbuf = LexBuffer<char>.FromString(query);
            Func<LexBuffer<char>, SqlParser.token> func = (x) => SqlLexer.tokenize(x);
            return SqlParser.startCT(FuncConvert.FromFunc(func), lexbuf);
        }

        public async IAsyncEnumerable<RowHolder> Execute(string queryText, ITransaction tran)
        {
            Sql.DmlDdlSqlStatement statement = BuildStatement(queryText);

            foreach (ISqlStatement handler in statementHandlers)
            {
                if (handler.ShouldExecute(statement))
                {
                    await foreach (RowHolder row in handler.Execute(statement, tran))
                    {
                        yield return row;
                    }

                    yield break;
                }
            }

            throw new ArgumentException();
        }
    }
}
