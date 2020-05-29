using PageManager;
using System.IO;
using System.Threading.Tasks;

namespace LogManager
{
    public class AllocatePageLogRecord : ILogRecord
    {
        public readonly ulong PageId;
        public readonly ulong TranId;
        public readonly PageType PageType;
        public readonly uint PageSize;
        public readonly ulong NextPageId;
        public readonly ulong PrevPageId;

        public LogRecordType GetRecordType() => LogRecordType.AllocatePage;

        public AllocatePageLogRecord(ulong pageId, ulong transactionid, PageType pageType, uint pageSize, ulong nextPageId, ulong prevPageId) =>
            (PageId, TranId, PageType, PageSize, NextPageId, PrevPageId) = (pageId, transactionid, pageType, pageSize, nextPageId, prevPageId);

        public RedoContent GetRedoContent()
        {
            throw new System.NotImplementedException();
        }

        public UndoContent GetUndoContent()
        {
            throw new System.NotImplementedException();
        }

        public Task Redo(IPageManager pageManager, ITransaction tran)
        {
            throw new System.NotImplementedException();
        }

        public void Serialize(BinaryWriter destination)
        {
            throw new System.NotImplementedException();
        }

        public ulong TransactionId() => this.TranId;

        public Task Undo(IPageManager pageManager, ITransaction tran) => Task.CompletedTask;
    }
}
