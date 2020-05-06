using NUnit.Framework;
using FSharp.Text.Lexing;
using FSharp.Text.Parsing;
using Microsoft.FSharp.Core;
using System;
using System.Linq;

namespace ParserLexerTests
{
    public class ParsingTests
    {
        [Test]
        public void ParsingExtractTest()
        {
            string query =
                @"SELECT x, y, z   
                FROM t1   
                LEFT JOIN t2   
                INNER JOIN t3 ON t3.ID = t2.ID   
                WHERE x = 50 AND y = 20   
                ORDER BY x ASC, y DESC, z";

            var lexbuf = LexBuffer<char>.FromString(query);
            Func<LexBuffer<char>, SqlParser.token> func = (x) => SqlLexer.tokenize(x);
            var f = FuncConvert.FromFunc(func);
            Sql.sqlStatement statement = SqlParser.start(f, lexbuf);

            Assert.AreEqual(new string[] { "x", "y", "z" }, statement.Columns.ToArray());
        }
    }
}