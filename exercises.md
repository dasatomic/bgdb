This is a list of exercises for students that would like to start with database development.

# Task 1. Init code and start bgdb repl.

REPL means Read-eval-print-loop. It represents a small wrapper around bgdb that allows user to pass commands and work with the rest of bgdb from command line.

Follow instructions in readme.md.
In Visual Studio start with ctrl + F5 to open bgdb repl.
Alternatively, from root of enlistment, go to bgdbRepl folder in command line:

`cd bgdbRepl`

and run:

`dotnet run`

or you can automatically create and load csv with precreated data:

`dotnet run --set_load_path .\datasets\titanic-passengers.csv`

Spend some time creating tables/querying and getting a feel on what works and what doesn't work.

# Task 2. Run tests.

Tests can be run from command line by running
`dotnet test` from enlistment root folder.

You can also use visual studio. After opening bgdb.sln go to Test > Test Explorer. There you will see the list off all the tests. Feel free to run any of them. You can also set a breakpoint in one of the tests (e.g. CreateTableE2E) and right click on the test > Debug. Through this process you can go through entire process step by step.

# Task 3. Create your branch.

To create your own branch first run:

`git checkout bgdb_exercises`


Then create your own branch under this one with:

`git checkout -b "MyName_Exercise_One"`

e.g.:

`git checkout -b "AleksandarTomic_Exercise_One"`

# Task 3. Update Lexer.

Currently the syntax that creates a table is following:

`CREATE TABLE TableName (column_definitions)`

e.g.

`CREATE TABLE MyTable (TYPE_INT a, TYPE_DOUBLE b)`

Let's say that we want to change this into:

`CREATE STORE someName (column_definitions)`

In ParserTests.cs you will find following test:

```cs
    [Test]
    [Ignore("You need to make this work.")]
    public void CreateTableRandomInput()
    {
        string query = "CREATE STORE myTable (TYPE_STRING(20) mycolumn)";

        var createStatement = GetCreateTableStatement(query);

        Assert.AreEqual("myTable", createStatement.Table);
        Assert.IsTrue(createStatement.ColumnList[0].Item1.IsStringCType);
        Assert.AreEqual(20, createStatement.ColumnList[0].Item2);
    }
```

Currently it is marked with Ignore flag which means that Test Framework will not propagate it's failures.

Your task is to make sure that this test is passing (it is ok to break other tests in the process :).

You don't really need to read this for this task, but if you want to know more about this you are welcome to start digging:

To learn more about parsers/lexers you can start with this wiki page:
https://en.wikipedia.org/wiki/Parsing

Parser generator used in wiki db is a derivative of Yacc and Lex:
https://en.wikipedia.org/wiki/Yacc

The concrete library used for bgdb parser is:
https://fsprojects.github.io/FsLexYacc/fslex.html

Note that Parser/Lexer layer are written in `F#`. For this task you don't need any `F#` knowledge so don't worry about that.

The code that you need to change is in `ParserLexerFSharp` folder.

  - <details><summary>Hint 1</summary>
    <pre>
    You will need to update SqlLexer.fsl file. There is a list of all keywords that have special meaning to the parser.
    </pre>
   </details>

  - <details><summary>Hint 2</summary>
    <pre>
    Grammar is in SqlParser.fsp. You will need to change that file as well.
    </pre>
   </details>

  - <details><summary>Hint 3</summary>
    <pre>
    If you see any errors while building the project after your changes, try to get parser/lexer output. You can just build this project by going to ParserLexerFsharp folder and running dotnet build from there.
    </pre>
   </details>

- <details><summary>Hint 4</summary>
    <pre>
    You can just blindly replace TABLE with STORE in SqlLexer.fsl and SqlParser.fsp
    </pre>
   </details>

Now rerun the CreateTableRandomInput. It should be passing. You can also see if the changes work from repl side:

```
>CREATE STORE MyStore (TYPE_INT a)

|



Total rows returned 0
>insert into MyStore values (42)

|



Total rows returned 0
>select * from mystore

| MYSTORE.A |
-------------
|        42 |
-------------

-------------
Total rows returned 1
>
```

When you are done commit your changes and push them to github. Once you are done we will chat about your solutions.