using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;

namespace PageManager
{
    public interface IPageSerializer<T, ST> : IPage
    {
        public bool CanFit(T items, ITransaction transaction);
        public uint GetSizeNeeded(T items);
        public void Merge(T items, ITransaction transaction);
        public void Update(ST item, ushort position, ITransaction transaction);
        public T Fetch(ITransaction tran);
    }

    public abstract class PageSerializerBase<S /* Storage */, T /* Interface Type */, ST> : IPageSerializer<T, ST>
    {
        protected uint pageSize;
        protected ulong pageId;
        protected ulong prevPageId;
        protected ulong nextPageId;
        protected uint rowCount;
        protected bool isDirty = false;
        protected S items;

        public ulong NextPageId() => this.nextPageId;
        public ulong PageId() => this.pageId;
        public ulong PrevPageId() => this.prevPageId;
        public uint SizeInBytes() => this.pageSize;
        protected uint GetRowCount(T items) => this.rowCount;
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

        public abstract void Merge(T items, ITransaction transaction);
        public abstract uint GetSizeNeeded(T items);
        public abstract bool CanFit(T items, ITransaction transaction);
        public abstract PageType PageType();
        public abstract void Persist(BinaryWriter destination);
        public abstract T Fetch(ITransaction tran);
        public abstract void RedoLog(ILogRecord record, ITransaction tran);
        public abstract void UndoLog(ILogRecord record, ITransaction tran);
        public abstract bool Equals([AllowNull] PageSerializerBase<S, T, ST> other, ITransaction tran);

        public bool IsDirty() => this.isDirty;

        public void ResetDirty()
        {
            this.isDirty = false;
        }

        public abstract void Update(ST item, ushort position, ITransaction transaction);
    }
}
