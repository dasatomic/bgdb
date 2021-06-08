using System;

namespace VideoProcessing
{
    public class FfProbeErrorOutputException : Exception
    {
        public FfProbeErrorOutputException(string errorTest) : base(errorTest)
        { }
    }
}
