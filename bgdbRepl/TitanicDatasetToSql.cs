using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace bgdbRepl
{
    public static class TitanicDatasetToSql
    {
        public static List<string> TitanicCsvToSql(string pathToCsv)
        {
            string[] lines = System.IO.File.ReadAllLines(pathToCsv);
            List<string> returnSql = new List<string>();

            // pos 0, 1, 2, 3 (name), 4, 5 (age), 6 (sib), 7 (parent), 11 (embarked)
            string createTable = @"CREATE TABLE Passengers (TYPE_INT PassengerId, TYPE_STRING(3) Survived, TYPE_INT Class, TYPE_STRING(70) Name, TYPE_STRING(6) Sex, TYPE_DOUBLE Age, TYPE_INT Siblings, TYPE_INT Parents, TYPE_STRING(1) EmbarkedPort)";
            returnSql.Add(createTable);

            for (int i = 1; i < lines.Length; i++)
            {
                string line = lines[i];
                string[] vals = line.Split(";");

                string[] charsToRemove = new string[] { "'", };

                string nameNormalized = vals[3];
                foreach (string rem in charsToRemove)
                {
                    nameNormalized = nameNormalized.Replace(rem, "");
                }

                string command = $"INSERT INTO Passengers VALUES ({vals[0]},'{vals[1]}', {vals[2]}, '{nameNormalized}', '{vals[4]}', {vals[5]}, {vals[6]}, {vals[7]}, '{vals[11]}')";
                returnSql.Add(command);
            }

            return returnSql;
        }
    }
}
