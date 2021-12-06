module Sql

type value =
    | Int       of int
    | Float     of float
    | String    of string
    | Id        of string

type aggType = Min | Max | Count | Sum

type scalarArgs =
    | Args1 of value 
    | Args2 of (value * value)
    | Args3 of (value * value * value)

type dir = Asc | Desc
type op = Eq | Gt | Ge | Lt | Le

type order = string * dir
type func = string * scalarArgs

type valueOrFunc =
    | Value of value
    | FuncCall of func

type where =
    | Cond of (valueOrFunc * op * valueOrFunc)
    | And of where * where
    | Or of where * where

type columnSelect =
    | Aggregate of (aggType * string)
    | ValueOrFunc of valueOrFunc

type selectType =
    | ColumnList of columnSelect list
    | Star

type joinType = Inner | Left | Right

type join = string * joinType * where option // table name, join, optional on clause

type sqlStatement =
    { 
        Top : int option
        From : sqlStatementOrId;
        Columns : selectType;
        Joins : join list;
        Where : where option
        GroupBy : string list;
        OrderBy : order list
    }
and sqlStatementOrId =
    | FromTable of string
    | FromSubquery of sqlStatement
    // Path + extension filter
    | FileSystemProvider of value 
    // Subquery + chunksize in seconds
    | VideoChunkProviderSubquery of sqlStatement * int
    | VideoImageProviderSubquery of sqlStatement * int * int

type columntype = IntCType | StringCType | DoubleCType
// columntype + rep count + name.
type columndef = columntype * int * string

type createTableStatement =
    {
        Table: string;
        ColumnList: columndef list;
        ClusteredIndexList: string list;
    }

type dropTableStatement =
    {
        Table: string;
    }

type insertStatement =
    {
        Table: string;
        Values: value list;
    }

type DmlDdlSqlStatement = 
    | Select of sqlStatement
    | Create of createTableStatement
    | Drop of string
    | Insert of insertStatement
