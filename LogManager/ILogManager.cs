using System;

namespace LogManager
{
    public interface ILogManager
    {
        public void CommitTransaction(ITransaction tran);
    }
}
