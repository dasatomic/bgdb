using LogManager;
using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;

namespace PageManager
{
    public abstract class SimpleTypeOnlyPage<T> : PageSerializerBase<T[]>
        where T : struct
    {
        private readonly PageType pageType;

        protected virtual uint FooterLenght() => 0;

        public SimpleTypeOnlyPage(uint pageSize, ulong pageId, PageType pageType, ulong prevPageId, ulong nextPageId, ITransaction transaction)
        {
            if (pageSize < IPage.FirstElementPosition + (uint)Marshal.SizeOf(default(T)))
            {
                throw new ArgumentException("Size can't be less than size of int");
            }

            if (pageSize % (uint)Marshal.SizeOf(default(T)) != 0)
            {
                throw new ArgumentException("Page size needs to be divisible with elem type");
            }

            this.pageSize = pageSize;
            this.pageId = pageId;
            this.pageType = pageType;
            this.nextPageId = nextPageId;
            this.prevPageId = prevPageId;

            ILogRecord logRecord = new AllocatePageLogRecord(pageId, transaction.TranscationId(), pageType, pageSize, nextPageId, prevPageId, null);
            transaction.AddRecord(logRecord);

            this.items = new T[0];
        }

        public SimpleTypeOnlyPage(BinaryReader stream, PageType pageType)
        {
            this.pageId = stream.ReadUInt64();
            this.pageSize = stream.ReadUInt32();

            PageType pageTypePersisted = (PageType)stream.ReadUInt32();

            if (pageType != pageTypePersisted)
            {
                throw new InvalidCastException();
            }

            this.rowCount = stream.ReadUInt32();

            this.prevPageId = stream.ReadUInt64();
            this.nextPageId = stream.ReadUInt64();

            if (stream.BaseStream.Position % this.pageSize != IPage.FirstElementPosition)
            {
                throw new SerializationException();
            }

            SerializeInternal(stream);
        }

        public override PageType PageType() => pageType;

        public override uint MaxRowCount()
        {
            return (this.pageSize - IPage.FirstElementPosition - this.FooterLenght()) / (uint)Marshal.SizeOf(default(T));
        }

        public override bool CanFit(T[] items)
        {
            return this.pageSize - IPage.FirstElementPosition - this.FooterLenght() - this.items.Length * (uint)Marshal.SizeOf(default(T))  >= (uint)Marshal.SizeOf(default(T)) * items.Length;
        }

        public override uint GetSizeNeeded(T[] items)
        {
            return (uint)items.Length * (uint)Marshal.SizeOf(default(T));
        }

        public override void Merge(T[] items, ITransaction transaction)
        {
            if (!CanFit(items))
            {
                throw new SerializationException();
            }

            this.items = this.items.Concat(items).ToArray();
            this.rowCount = (uint)this.items.Length;

            // TODO: Need to fire transaction here.
        }

        public override T[] Fetch()
        {
            return this.items;
        }

        protected abstract void SerializeInternal(BinaryReader stream);

        public override bool Equals(PageSerializerBase<T[]> other)
        {
            if (this.pageId != other.PageId())
            {
                return false;
            }

            if (this.MaxRowCount() != other.MaxRowCount())
            {
                return false;
            }

            if (this.PrevPageId() != other.PrevPageId())
            {
                return false;
            }

            if (this.NextPageId() != other.NextPageId())
            {
                return false;
            }

            if (!Enumerable.SequenceEqual(this.Fetch(), other.Fetch()))
            {
                return false;
            }

            return true;
        }
    }
}
