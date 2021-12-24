using BenchmarkDotNet.Running;

namespace PerfExperiments
{
    class Program
    {
        static void Main(string[] args)
        {
            /*
            var ss = new SimdSum();
            ss.ItemNum = 1000;
            ss.CreateArray();
            //ss.VectorSum();
            ss.Intrinsics();
            */

            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        }
    }
}
