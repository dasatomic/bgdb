using LogManager;
using System;
using System.Collections.Generic;
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
        public T FetchWithOffset(uint offset, ITransaction tran);
    }

    public class StringOnlyPage : PageSerializerBase<char[], IEnumerable<char[]>, char[]>, IPageWithOffsets<char[]>
    {
        public StringOnlyPage(uint pageSize, ulong pageId, ulong prevPageId, ulong nextPageId, ITransaction tran)
        {
            if (pageSize < IPage.FirstElementPosition + sizeof(ushort))
            {
                throw new ArgumentException("Size can't be less than size of char + sizeof(ushort) for length.");
            }

            this.pageSize = pageSize;
            this.pageId = pageId;
            this.prevPageId = prevPageId;
            this.nextPageId = nextPageId;
            this.items = new char[this.MaxRowCount()];

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

            this.items = new char[pageSize - IPage.FirstElementPosition];

            Array.Copy(stream.ReadChars((int)this.rowCount), this.items, this.rowCount);

            this.isDirty = false;
        }

        private (char lower, char upper) Int16ToCharPair(Int16 size) => ((char)size, (char)(size >> 8));
        private Int16 CharPairToSize(char lower, char upper) => (Int16)(((Int16)upper << 8) + lower);

        public override PageType PageType() => global::PageManager.PageType.StringPage;

        public override uint MaxRowCount()
        {
            return this.pageSize - IPage.FirstElementPosition;
        }

        public override bool CanFit(char[] item, ITransaction transaction)
        {
            transaction.VerifyLock(this.pageId, LockManager.LockTypeEnum.Shared);
            uint size = (uint)(this.rowCount + item.Length + sizeof(ushort));
            return this.pageSize - IPage.FirstElementPosition >= size;
        }

        public override uint RowCount()
        {
            int posInBuffer = 0;
            int totalItems = 0;
            while (posInBuffer < this.rowCount)
            {
                posInBuffer += this.CharPairToSize(this.items[posInBuffer], this.items[posInBuffer + 1]) + sizeof(ushort);
                totalItems++;
            }

            return (uint)totalItems;
        }

        public override int Insert(char[] itemToInsert, ITransaction transaction)
        {
            transaction.VerifyLock(this.pageId, LockManager.LockTypeEnum.Exclusive);
            uint size = this.rowCount + (uint)itemToInsert.Length + sizeof(short);

            if (this.pageSize - IPage.FirstElementPosition < size)
            {
                throw new NotEnoughSpaceException();
            }

            uint positionInBuffer = this.rowCount;

            int startPos = (int)this.rowCount;
            this.rowCount += (uint)itemToInsert.Length + sizeof(short);

            (char lowSize, char highSize) = this.Int16ToCharPair((short)itemToInsert.Length);
            this.items[positionInBuffer] = lowSize;
            this.items[positionInBuffer + 1] = highSize;
            Array.Copy(itemToInsert, 0, this.items, positionInBuffer + sizeof(short), itemToInsert.Length);

            byte[] bs = new byte[itemToInsert.Length + sizeof(short)];
            using (MemoryStream ms = new MemoryStream(bs))
            using (BinaryWriter bw = new BinaryWriter(ms))
            {
                bw.Write((ushort)itemToInsert.Length);
                bw.Write(itemToInsert);
                ILogRecord rc = new InsertRowRecord(this.pageId, (ushort)(startPos), bs, transaction.TranscationId(), this.PageType());
                transaction.AddRecord(rc);
            }

            this.isDirty = true;

            return (int)positionInBuffer;
        }

        public char[] FetchWithOffset(uint offset, ITransaction transaction)
        {
            transaction.VerifyLock(this.pageId, LockManager.LockTypeEnum.Shared);
            if (offset >= this.pageSize - IPage.FirstElementPosition)
            {
                throw new ArgumentException();
            }

            short size = this.CharPairToSize(this.items[offset], this.items[offset + 1]);

            // TODO: Consider making span part of this API to remove need for copying the data.
            return this.items.AsSpan((int)offset + sizeof(short), size).ToArray();
        }

        public override void Persist(BinaryWriter destination)
        {
            destination.Write(this.pageId);
            destination.Write(this.pageSize);
            destination.Write((int)this.PageType());
            destination.Write(this.rowCount);
            destination.Write(this.prevPageId);
            destination.Write(this.nextPageId);
            destination.Write(this.items);
        }

        public override IEnumerable<char[]> Fetch(ITransaction tran)
        {
            tran.VerifyLock(this.pageId, LockManager.LockTypeEnum.Shared);

            int posInBuffer = 0;
            while (posInBuffer < this.rowCount)
            {
                short size = this.CharPairToSize(this.items[posInBuffer], this.items[posInBuffer + 1]);
                yield return this.items.AsSpan(posInBuffer + sizeof(ushort), size).ToArray();
                posInBuffer += size + sizeof(short);
            }
        }

        public bool CanFit(char[] item)
        {
            uint size = (uint)(this.rowCount + item.Length + sizeof(ushort));
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
                    // TODO: This is not working properly.
                    // As part of update work this needs to be updated.
                    // If size of previous entry is different from new one this leads to corruption.
                    short elemSize = br.ReadInt16();
                    (char lower, char higher) = this.Int16ToCharPair(elemSize);
                    this.items[redoContent.RowPosition] = lower;
                    this.items[redoContent.RowPosition + 1] = higher;
                    Array.Copy(br.ReadChars(elemSize), 0, this.items, redoContent.RowPosition + sizeof(short), elemSize);
                }
                else if (record.GetRecordType() == LogRecordType.RowInsert)
                {
                    if (redoContent.RowPosition != items.Length)
                    {
                        throw new LogCorruptedException();
                    }

                    short elemSize = br.ReadInt16();
                    (char lower, char higher) = this.Int16ToCharPair(elemSize);

                    this.items[this.rowCount] = lower;
                    this.items[this.rowCount + 1] = higher;

                    Array.Copy(items, 0, this.items, this.rowCount + sizeof(short), items.Length);
                    this.rowCount += sizeof(short) + (uint)items.Length;
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
                    // TODO: This is not working properly.
                    // As part of update work this needs to be updated.
                    // If size of previous entry is different from new one this leads to corruption.
                    short elemSize = br.ReadInt16();
                    (char lower, char higher) = this.Int16ToCharPair(elemSize);
                    this.items[undoContent.RowPosition] = lower;
                    this.items[undoContent.RowPosition + 1] = higher;
                    Array.Copy(br.ReadChars(elemSize), 0, this.items, undoContent.RowPosition + sizeof(short), elemSize);
                }
            }
            else if (record.GetRecordType() == LogRecordType.RowInsert)
            {
                // TODO: Need to figure how to find the insert location.
                this.rowCount = undoContent.RowPosition;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public override bool Equals([AllowNull] PageSerializerBase<char[], IEnumerable<char[]>, char[]> other, ITransaction tran)
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

            if (this.Fetch(tran).Count() != other.Fetch(tran).Count())
            {
                return false;
            }

            if (!Enumerable.SequenceEqual(this.Fetch(tran), other.Fetch(tran)))
            {
                return false;
            }

            return true;
        }

        public override void Update(char[] item, ushort position, ITransaction transaction)
        {
            throw new NotImplementedException();
        }

        public override void At(ushort position, ITransaction tran, ref char[] item)
        {
            throw new NotImplementedException();
        }
    }
}
