using Newtonsoft.Json;
using System.Threading;
using System.Threading.Tasks;

namespace VideoProcessing
{

    public interface IExeRunner
    {
        public Task<FfProbeOutputSerializer> Execute(string args, CancellationToken token);
    }
}
