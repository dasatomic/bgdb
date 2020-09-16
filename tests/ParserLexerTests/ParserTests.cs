using NUnit.Framework;
using FSharp.Text.Lexing;
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
            Sql.DmlDdlSqlStatement statement = SqlParser.startCT(FuncConvert.FromFunc(func), lexbuf);
            Assert.IsTrue(statement.IsSelect);

            var selectStatement = ((Sql.DmlDdlSqlStatement.Select)statement).Item;

            Assert.AreEqual(new string[] { "x", "y", "z" }, selectStatement.Columns.ToArray());
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
            Sql.DmlDdlSqlStatement statement = SqlParser.startCT(FuncConvert.FromFunc(func), lexbuf);
            Assert.IsTrue(statement.IsSelect);

            var selectStatement = ((Sql.DmlDdlSqlStatement.Select)statement).Item;

            Assert.AreEqual(new string[] { "x", "y", "z" }, selectStatement.Columns.ToArray());
            Assert.AreEqual("t1", selectStatement.Table);

            Sql.where whereStatement = selectStatement.Where.Value;
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

        [Test]
        public void CreateTableTest()
        {
            string query = "CREATE TABLE mytable (TYPE_INT A, TYPE_INT B, TYPE_STRING(10) C)";

            var lexbuf = LexBuffer<char>.FromString(query);
            Func<LexBuffer<char>, SqlParser.token> func = (x) => SqlLexer.tokenize(x);
            
            var f = FuncConvert.FromFunc(func);
            Sql.DmlDdlSqlStatement statement = SqlParser.startCT(FuncConvert.FromFunc(func), lexbuf);

            Assert.IsTrue(statement.IsCreate);

            var createStatement = ((Sql.DmlDdlSqlStatement.Create)statement).Item;

            Assert.AreEqual("mytable", createStatement.Table);
            Assert.IsTrue(createStatement.ColumnList[0].Item1.IsIntCType);
            Assert.IsTrue(createStatement.ColumnList[1].Item1.IsIntCType);
            Assert.IsTrue(createStatement.ColumnList[2].Item1.IsStringCType);
            Assert.AreEqual(10, createStatement.ColumnList[2].Item2);
            Assert.AreEqual(new string[] { "A", "B", "C" }, createStatement.ColumnList.Select(cl => cl.Item3).ToArray());
        }

        [Test]
        public void InsertStatementTests()
        {
            string query = "INSERT INTO mytable VALUES (17,b,1.1)";

            var lexbuf = LexBuffer<char>.FromString(query);
            Func<LexBuffer<char>, SqlParser.token> func = (x) => SqlLexer.tokenize(x);
            
            var f = FuncConvert.FromFunc(func);
            Sql.DmlDdlSqlStatement statement = SqlParser.startCT(FuncConvert.FromFunc(func), lexbuf);

            Assert.IsTrue(statement.IsInsert);

            var insertStatement = ((Sql.DmlDdlSqlStatement.Insert)statement).Item;

            Assert.AreEqual("mytable", insertStatement.Table);
            Assert.IsTrue(insertStatement.Values[0].IsInt);
            Assert.IsTrue(insertStatement.Values[1].IsString);
            Assert.IsTrue(insertStatement.Values[2].IsFloat);

            Assert.AreEqual(((Sql.value.Int)insertStatement.Values[0]).Item, 17);
            Assert.AreEqual(((Sql.value.String)insertStatement.Values[1]).Item, "b");
            Assert.AreEqual(((Sql.value.Float)insertStatement.Values[2]).Item, 1.1);
        }
    }
}