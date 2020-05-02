using System;
using System.Runtime.Serialization;

namespace PageManager
{
    public interface IPageSerializer<T> : IPage
    {
        public void Serialize(T items);
        public T Deserialize();
        public bool CanFit(T items);
        public uint GetSizeNeeded(T items);
        public void Merge(T items);
    }

    public abstract class PageSerializerBase<T> : IPageSerializer<T>
    {
        protected uint pageSize;
        protected ulong pageId;
        protected ulong prevPageId;
        protected ulong nextPageId;
        protected uint rowCount;

        protected byte[] content;
        public abstract bool CanFit(T items);
        public abstract T Deserialize();
        public byte[] GetContent() => this.content;
        public abstract uint MaxRowCount();
        public ulong NextPageId() => this.nextPageId;
        public ulong PageId() => this.pageId;
        public abstract PageType PageType();
        public ulong PrevPageId() => this.prevPageId;

        public void Serialize(T items)
        {
            uint neededSize = this.GetSizeNeeded(items);
            if (neededSize > this.pageSize - IPage.FirstElementPosition)
            {
                throw new SerializationException();
            }

            uint contentPosition = 0;
            foreach (byte pageByte in BitConverter.GetBytes(this.pageId))
            {
                content[contentPosition] = pageByte;
                contentPosition++;
            }

            foreach (byte sizeByte in BitConverter.GetBytes(this.pageSize))
            {
                content[contentPosition] = sizeByte;
                contentPosition++;
            }

            foreach (byte typeByte in BitConverter.GetBytes((int)PageManager.PageType.MixedPage))
            {
                content[contentPosition] = typeByte;
                contentPosition++;
            }

            this.rowCount = this.GetRowCount(items);
            foreach (byte numOfRowsByte in BitConverter.GetBytes(this.rowCount))
            {
                content[contentPosition] = numOfRowsByte;
                contentPosition++;
            }

            SerializeInternal(items);
        }

        public uint SizeInBytes() => this.pageSize;
        public abstract uint GetSizeNeeded(T items);
        protected abstract uint GetRowCount(T items);
        protected abstract void SerializeInternal(T items);
        public void SetNextPageId(ulong nextPageId) => this.nextPageId = nextPageId;
        public void SetPrevPageId(ulong prevPageId) => this.prevPageId = prevPageId;

        public uint RowCount() => this.rowCount;
        public abstract void Merge(T items);
    }
}
