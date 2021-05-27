using System;
using System.Collections.Generic;
using System.Text;

namespace QueryProcessing.Exceptions
{
    public class InvalidFunctionArgument : Exception
    {
        public InvalidFunctionArgument(string message): base(message)
        {
        }
    }
}
