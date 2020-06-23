using LogManager;
using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace PageManager
{
    public interface IAllocateStringPage
    {
        Task<StringOnlyPage> AllocatePageStr(ulong prevPage, ulong nextPage, ITransaction tran);
        Task<StringOnlyPage> GetPageStr(ulong pageId, ITransaction tran);
    }

    public interface IPageWithOffsets<T>
    {
        public uint MergeWithOffsetFetch(T item);
        public T FetchWithOffset(uint offset);
        public bool CanFit(T item);
    }

    public class StringOnlyPage : PageSerializerBase<char[][]>, IPageWithOffsets<char[]>
    {
        public StringOnlyPage(uint pageSize, ulong pageId, ulong prevPageId, ulong nextPageId, ITransaction tran)
        {
            if (pageSize < IPage.FirstElementPosition + sizeof(char) * 2)
            {
                throw new ArgumentException("Size can't be less than size of char and null termination");
            }

            this.pageSize = pageSize;
            this.pageId = pageId;
            this.prevPageId = prevPageId;
            this.nextPageId = nextPageId;
            this.items = new char[0][];

            ILogRecord logRecord = new AllocatePageLogRecord(pageId, tran.TranscationId(), global::PageManager.PageType.StringPage, pageSize, nextPageId, prevPageId, null);
            tran.AddRecord(logRecord);

            this.isDirty = true;
        }

        public StringOnlyPage(BinaryReader stream)
        {
            this.pageId = stream.ReadUInt64();
            this.pageSize = stream.ReadUInt32();

            PageType pageTypePersisted = (PageType)stream.ReadUInt32();

            if (global::PageManager.PageType.StringPage != pageTypePersisted)
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

            this.items = new char[this.rowCount][];

            for (int elemCount = 0; elemCount < this.rowCount; elemCount++)
            {
                int charLength = stream.ReadInt16();
                this.items[elemCount] = stream.ReadChars(charLength);
            }

            this.isDirty = false;
        }

        public override PageType PageType() => global::PageManager.PageType.StringPage;

        public override uint GetSizeNeeded(char[][] items)
        {
            uint byteCount = 0;
            foreach (char[] item in items)
            {
                byteCount += (uint)item.Length + sizeof(short);
            }

            return byteCount;
        }

        public override uint MaxRowCount()
        {
            return this.pageSize - IPage.FirstElementPosition;
        }

        public override bool CanFit(char[][] items)
        {
            uint size = this.GetSizeNeeded(this.items) + this.GetSizeNeeded(items);
            return this.pageSize - IPage.FirstElementPosition >= size;
        }

        public override void Merge(char[][] items, ITransaction transaction)
        {
            uint size = this.GetSizeNeeded(this.items) + this.GetSizeNeeded(items);
            if (this.pageSize - IPage.FirstElementPosition < size)
            {
                throw new NotEnoughSpaceException();
            }

            int startPos = this.items.Length;
            this.items = this.items.Concat(items).ToArray();
            this.rowCount = (uint)this.items.Length;

            for (int i = 0; i < items.Length; i++)
            {
                byte[] bs = new byte[items[i].Length + 2];
                using (MemoryStream ms = new MemoryStream(bs))
                using (BinaryWriter bw = new BinaryWriter(ms))
                {
                    bw.Write((ushort)items[i].Length);
                    bw.Write(items[i]);
                    ILogRecord rc = new InsertRowRecord(this.pageId, (ushort)(startPos + i), bs, transaction.TranscationId(), this.PageType());
                    transaction.AddRecord(rc);
                }
            }

            this.isDirty = true;
        }

        public uint MergeWithOffsetFetch(char[] item)
        {
            uint size = this.GetSizeNeeded(this.items) + (uint)item.Length + sizeof(short);

            if (this.pageSize - IPage.FirstElementPosition < size)
            {
                throw new NotEnoughSpaceException();
            }

            uint positionInBuffer = IPage.FirstElementPosition;
            foreach (var arr in this.items)
            {
                positionInBuffer += (uint)arr.Length + sizeof(short);
            }

            this.rowCount++;
            this.items = this.items.Append(item).ToArray();

            this.isDirty = true;

            return positionInBuffer;
        }

        public char[] FetchWithOffset(uint offset)
        {
            if (offset < IPage.FirstElementPosition || offset >= this.pageSize)
            {
                throw new ArgumentException();
            }

            uint currOfset = IPage.FirstElementPosition;
            foreach (char[] item in this.items)
            {
                if (currOfset == offset)
                {
                    return item;
                }
                else
                {
                    currOfset += (uint)item.Length + sizeof(short);
                }
            }

            throw new PageCorruptedException();
        }

        public override void Persist(BinaryWriter destination)
        {
            if (this.rowCount != this.items.Length)
            {
                throw new PageCorruptedException();
            }

            destination.Write(this.pageId);
            destination.Write(this.pageSize);
            destination.Write((int)this.PageType());
            destination.Write(this.rowCount);
            destination.Write(this.prevPageId);
            destination.Write(this.nextPageId);

            foreach (char[] item in this.items)
            {
                destination.Write((short)item.Length);
                destination.Write(item);
            }
        }

        public override char[][] Fetch(ITransaction tran) => this.items;

        public bool CanFit(char[] item)
        {
            uint size = this.GetSizeNeeded(this.items) + (uint)item.Length + sizeof(short);
            return this.pageSize - IPage.FirstElementPosition >= size;
        }

        public override void RedoLog(ILogRecord record, ITransaction tran)
        {
            var redoContent = record.GetRedoContent();
            using (MemoryStream ms = new MemoryStream(redoContent.DataToApply))
            using (BinaryReader br = new BinaryReader(ms))
            {
                if (record.GetRecordType() == LogRecordType.RowModify)
                {
                    int elemSize = br.ReadUInt16();
                    this.items[redoContent.RowPosition] = br.ReadChars(elemSize);
                }
                else if (record.GetRecordType() == LogRecordType.RowInsert)
                {
                    if (redoContent.RowPosition != items.Length)
                    {
                        throw new LogCorruptedException();
                    }

                    int elemSize = br.ReadUInt16();
                    this.items = this.items.Concat(new char[][] { br.ReadChars(elemSize) }).ToArray();
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
        }

        public override void UndoLog(ILogRecord record, ITransaction tran)
        {
            var undoContent = record.GetUndoContent();
            if (record.GetRecordType() == LogRecordType.RowModify)
            {
                using (MemoryStream ms = new MemoryStream(undoContent.DataToUndo))
                using (BinaryReader br = new BinaryReader(ms))
                {
                    int elemSize = br.ReadUInt16();
                    this.items[undoContent.RowPosition] = br.ReadChars(elemSize);
                }
            }
            else if (record.GetRecordType() == LogRecordType.RowInsert)
            {
                this.items = this.items.Take(this.items.Length - 1).ToArray();
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public override bool Equals([AllowNull] PageSerializerBase<char[][]> other, ITransaction tran)
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

            if (this.Fetch(tran).Length != other.Fetch(tran).Length)
            {
                return false;
            }

            foreach (var pairs in this.Fetch(tran).Zip(other.Fetch(tran)))
            {
                if (Enumerable.SequenceEqual(pairs.First, pairs.Second))
                {
                    return false;
                }
            }

            return true;
        }

        public override void Update(char[][] item, ushort position, ITransaction transaction)
        {
            throw new NotImplementedException();
        }
    }
}
