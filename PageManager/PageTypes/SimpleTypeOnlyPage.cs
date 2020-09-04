using LogManager;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;

namespace PageManager
{
    public abstract class SimpleTypeOnlyPage<T> : PageSerializerBase<T[], IEnumerable<T>, T>
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

            this.isDirty = true;
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

            this.isDirty = false;
        }

        public override PageType PageType() => pageType;

        public override uint MaxRowCount()
        {
            return (this.pageSize - IPage.FirstElementPosition - this.FooterLenght()) / (uint)Marshal.SizeOf(default(T));
        }

        public override bool CanFit(T item, ITransaction transaction)
        {
            transaction.VerifyLock(this.pageId, LockManager.LockTypeEnum.Shared);
            return this.pageSize - IPage.FirstElementPosition - this.FooterLenght() - this.items.Length * (uint)Marshal.SizeOf(default(T))  >= (uint)Marshal.SizeOf(default(T));
        }

        protected abstract byte[] SerializeItem(T item);

        public override int Insert(T item, ITransaction transaction)
        {
            transaction.VerifyLock(this.pageId, LockManager.LockTypeEnum.Exclusive);

            if (!CanFit(item, transaction))
            {
                throw new SerializationException();
            }

            // TODO: This needs to go away.
            int startPos = this.items.Length;
            this.items = this.items.Concat(new T[] { item }).ToArray();
            this.rowCount = (uint)this.items.Length;

            byte[] bs = SerializeItem(item);
            ILogRecord rc = new InsertRowRecord(this.pageId, (ushort)(startPos), bs, transaction.TranscationId(), this.pageType);
            transaction.AddRecord(rc);

            this.isDirty = true;
            return startPos;
        }

        public override void Update(T item, ushort position, ITransaction transaction)
        {
            transaction.VerifyLock(this.pageId, LockManager.LockTypeEnum.Exclusive);

            if (position >= this.rowCount)
            {
                throw new PageCorruptedException();
            }

            byte[] oldVal = SerializeItem(this.items[position]);
            this.items[position] = item;
            byte[] newVal = SerializeItem(this.items[position]);

            ILogRecord rc = new UpdateRowRecord(this.pageId, position, oldVal, newVal, transaction.TranscationId(), this.pageType);
            transaction.AddRecord(rc);
        }

        public override IEnumerable<T> Fetch(ITransaction tran)
        {
            tran.VerifyLock(this.pageId, LockManager.LockTypeEnum.Shared);
            return this.items;
        }

        protected abstract void SerializeInternal(BinaryReader stream);

        public override bool Equals(PageSerializerBase<T[], IEnumerable<T>, T> other, ITransaction tran)
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

            if (!Enumerable.SequenceEqual(this.Fetch(tran), other.Fetch(tran)))
            {
                return false;
            }

            return true;
        }
    }
}
