using QueryProcessing.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryProcessing
{
    /// <summary>
    /// This is, hopefully, a temporary way around
    /// parser/lexer problems.
    /// Goal is to uppercase string on input, but leave string literals intact.
    /// Secondly, we need to substitute all special characters in literals for parsing
    /// and push them back after constructing the statement.
    /// </summary>
    public class InputStringNormalizer
    {
        private Dictionary<string, List<(int pos, char old)>> replacementDictionary;
        private char[] substitutions = new[] { ' ', '.', ',', '(', ')', '\"', '\\', ';' };

        public string InputForLexer { get; private set; }

        public InputStringNormalizer(string input)
        {
            bool captureStart = false;
            int startPosition = 0;
            char[] inputChars = input.ToCharArray();

            List<(int pos, char old)> replacementList = new List<(int pos, char old)>();
            this.replacementDictionary = new Dictionary<string, List<(int pos, char old)>>();


            for (int i = 0; i < inputChars.Length; i++)
            {
                if (inputChars[i] == '\'' && captureStart == false)
                {
                    startPosition = i;
                    captureStart = true;
                } else if (inputChars[i] == '\'' && captureStart == true)
                {
                    captureStart = false;
                    char[] tokenChars = new char[i - startPosition - 1];
                    for (int j = 0; j < tokenChars.Length; j++)
                    {
                        tokenChars[j] = inputChars[startPosition + 1 + j];
                    }

                    string currentToken = new string(tokenChars);
                    this.replacementDictionary.Add(currentToken, replacementList);
                    replacementList = new List<(int pos, char old)>();
                }

                if (captureStart == true)
                {
                    if (this.substitutions.Contains(inputChars[i]))
                    {
                        replacementList.Add((i - startPosition - 1, inputChars[i]));
                        inputChars[i] = 'x';
                    }
                }
                else
                {
                    inputChars[i] = Char.ToUpper(inputChars[i]);
                }
            }

            string result = new string(inputChars);

            if (result.StartsWith("CREATE TABLE TABLE"))
            {
                throw new InvalidTableNameException();
            }

            this.InputForLexer = result;
        }

        public string ApplyReplacementTokens(string input)
        {
            if (this.replacementDictionary.TryGetValue(input, out var replacements))
            {
                char[] valueChars = input.ToCharArray();
                foreach (var repl in replacements)
                {
                    valueChars[repl.pos] = repl.old;
                }

                return new string(valueChars);
            }

            return input;
        }

        public Sql.value ApplyReplacementTokens(Sql.value input)
        {
            if (input.IsString)
            {
                string norm = this.ApplyReplacementTokens(((Sql.value.String)input).Item);
                return Sql.value.NewString(norm);
            }

            return input;
        }
    }
}
