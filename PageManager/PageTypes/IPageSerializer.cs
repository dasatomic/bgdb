using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;

namespace PageManager
{
    public interface IPageSerializer<ST> : IPage
    {
        public bool CanFit(ST items, ITransaction transaction);
        public int Insert(ST items, ITransaction transaction);
        public void Update(ST item, ushort position, ITransaction transaction);
        public IEnumerable<ST> Fetch(ITransaction tran);
        public void At(ushort position, ITransaction tran, ref ST item);
    }

    public abstract class PageSerializerBase<S /* Storage */, ST> : IPageSerializer<ST>
    {
        protected uint pageSize;
        protected ulong pageId;
        protected ulong prevPageId;
        protected ulong nextPageId;
        protected uint rowCount;
        protected bool isDirty = false;
        protected S items;
        protected ulong bufferPoolToken;

        // TODO: not really optimal to do heap alloc anywhere in page.
        protected object lockObject = new object();

        public ulong NextPageId() => this.nextPageId;
        public ulong PageId() => this.pageId;
        public ulong PrevPageId() => this.prevPageId;
        public uint SizeInBytes() => this.pageSize;
        public void SetNextPageId(ulong nextPageId)
        {
            this.nextPageId = nextPageId;
            this.isDirty = true;
        }

        public void SetPrevPageId(ulong prevPageId)
        {
            this.prevPageId = prevPageId;
            this.isDirty = true;
        }

        public virtual uint RowCount() => this.rowCount;

        // Abstract fields.
        public abstract uint MaxRowCount();

        public abstract bool CanFit(ST items, ITransaction transaction);
        public abstract PageType PageType();
        public abstract void Persist(BinaryWriter destination);
        public abstract IEnumerable<ST> Fetch(ITransaction tran);
        public abstract void RedoLog(ILogRecord record, ITransaction tran);
        public abstract void UndoLog(ILogRecord record, ITransaction tran);
        public abstract bool Equals([AllowNull] PageSerializerBase<S, ST> other, ITransaction tran);

        public bool IsDirty() => this.isDirty;

        public void ResetDirty()
        {
            this.isDirty = false;
        }

        public abstract void Update(ST item, ushort position, ITransaction transaction);
        public abstract int Insert(ST item, ITransaction transaction);
        public abstract void At(ushort position, ITransaction tran, ref ST item);

        public ulong GetBufferPoolToken() => this.bufferPoolToken;

        public void TakeLatch()
        {
            bool lockTaken = false;
            System.Threading.Monitor.Enter(this.lockObject, ref lockTaken);

            if (!lockTaken)
            {
                throw new Exceptions.UnableToAcquireLatch();
            }
        }

        public void ReleaseLatch()
        {
            System.Threading.Monitor.Exit(this.lockObject);
        }
    }
}
