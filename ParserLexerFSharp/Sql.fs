module Sql

type value =
    | Int of int
    | Float of float
    | String of string

type dir = Asc | Desc
type op = Eq | Gt | Ge | Lt | Le

type order = string * dir

type where =
    | Cond of (value * op * value)
    | And of where * where
    | Or of where * where

type joinType = Inner | Left | Right

type join = string * joinType * where option // table name, join, optional on clause

type sqlStatement =
    { 
        Table : string;
        Columns : string list;
        Joins : join list;
        Where : where option
        OrderBy : order list
    }

type column = string * string

type createTableStatement =
    {
        Table: string;
    }
