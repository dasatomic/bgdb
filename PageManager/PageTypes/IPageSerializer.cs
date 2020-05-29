using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.Serialization;

namespace PageManager
{
    public interface IPageSerializer<T> : IPage
    {
        public void Store(T items);
        public void Persist(Stream destination);
        public bool CanFit(T items);
        public uint GetSizeNeeded(T items);
        public void Merge(T items);
        public T Fetch();
    }

    public abstract class PageSerializerBase<T> : IPageSerializer<T>, IEquatable<PageSerializerBase<T>>
    {
        protected uint pageSize;
        protected ulong pageId;
        protected ulong prevPageId;
        protected ulong nextPageId;
        protected uint rowCount;
        protected T items;

        public ulong NextPageId() => this.nextPageId;
        public ulong PageId() => this.pageId;
        public ulong PrevPageId() => this.prevPageId;
        public uint SizeInBytes() => this.pageSize;
        protected uint GetRowCount(T items) => this.rowCount;
        public void SetNextPageId(ulong nextPageId) => this.nextPageId = nextPageId;
        public void SetPrevPageId(ulong prevPageId) => this.prevPageId = prevPageId;
        public uint RowCount() => this.rowCount;

        // Abstract fields.
        public abstract uint MaxRowCount();

        public abstract void Merge(T items);
        public abstract uint GetSizeNeeded(T items);
        public abstract void Store(T items);
        public abstract bool CanFit(T items);
        public abstract PageType PageType();
        public abstract void Persist(Stream destination);
        public abstract T Fetch();
        public abstract void RedoLog(ILogRecord record, ITransaction tran);
        public abstract void UndoLog(ILogRecord record, ITransaction tran);
        public abstract bool Equals([AllowNull] PageSerializerBase<T> other);
    }
}
