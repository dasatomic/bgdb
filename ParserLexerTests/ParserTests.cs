using NUnit.Framework;
using FSharp.Text.Lexing;
using FSharp.Text.Parsing;
using Microsoft.FSharp.Core;
using System;
using System.Linq;
using System.Security.Cryptography.X509Certificates;

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

        [Test]
        public void ParsingWhereClause()
        {
            string query =
                @"SELECT x, y, z   
                FROM t1   
                WHERE x = 50 AND y = 20";

            var lexbuf = LexBuffer<char>.FromString(query);
            Func<LexBuffer<char>, SqlParser.token> func = (x) => SqlLexer.tokenize(x);
            var f = FuncConvert.FromFunc(func);
            Sql.sqlStatement statement = SqlParser.start(f, lexbuf);

            Assert.AreEqual(new string[] { "x", "y", "z" }, statement.Columns.ToArray());
            Assert.AreEqual("t1", statement.Table);

            Sql.where whereStatement = statement.Where.Value;
            Assert.IsTrue(whereStatement.IsAnd);

            Sql.where.And andStatement = (Sql.where.And)whereStatement;
            Assert.IsTrue(andStatement.Item1.IsCond);
            Assert.IsTrue(andStatement.Item2.IsCond);

            Sql.where.Cond leftCond = (Sql.where.Cond)andStatement.Item1;
            Sql.where.Cond rightCond = (Sql.where.Cond)andStatement.Item2;

            Assert.IsTrue(leftCond.Item.Item1.IsString);
            Assert.IsTrue(((Sql.value.String)leftCond.Item.Item1).Item == "x");
            Assert.IsTrue(leftCond.Item.Item2.IsEq);
            Assert.IsTrue(leftCond.Item.Item3.IsInt);
            Assert.IsTrue(((Sql.value.Int)leftCond.Item.Item3).Item == 50);

            Assert.IsTrue(rightCond.Item.Item1.IsString);
            Assert.IsTrue(((Sql.value.String)rightCond.Item.Item1).Item == "y");
            Assert.IsTrue(rightCond.Item.Item2.IsEq);
            Assert.IsTrue(rightCond.Item.Item3.IsInt);
            Assert.IsTrue(((Sql.value.Int)rightCond.Item.Item3).Item == 20);
        }
    }
}