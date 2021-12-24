using BenchmarkDotNet.Running;
using System.Management;
using System.Numerics;
using System.Text;

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
            ss.IntrinsicsInt();
            */

            var output = new StringBuilder();
            output.AppendLine($"Starting execution at {DateTime.Now} on {Environment.MachineName} running {Environment.OSVersion.VersionString}");

            var currentLine = $"cores: {Environment.ProcessorCount}";
            output.AppendLine(currentLine);
            Console.WriteLine(currentLine);

            var simdLength = Vector<double>.Count;
            var simdAvailable = Vector.IsHardwareAccelerated;

            currentLine = $"CPU SIMD instructions present: {simdAvailable}";
            output.AppendLine(currentLine);
            Console.WriteLine(currentLine);

            currentLine = $"CPU SIMD length: {sizeof(double) * simdLength * 8} bits = {simdLength} of {typeof(double).FullName} ({sizeof(double) * 8} bits each)";
            output.AppendLine(currentLine);
            Console.WriteLine(currentLine);

            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        }
    }
}
