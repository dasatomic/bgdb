using PageManager;
using System.Threading.Tasks;

namespace LogManager
{
    public interface ILogManager
    {
        public Task CommitTransaction(ITransaction tran);
    }
}
