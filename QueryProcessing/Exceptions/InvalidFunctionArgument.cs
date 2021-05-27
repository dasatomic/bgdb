using System;

namespace QueryProcessing.Exceptions
{
    public class InvalidFunctionArgument : Exception
    {
        public InvalidFunctionArgument(string message): base(message)
        {
        }
    }
}
