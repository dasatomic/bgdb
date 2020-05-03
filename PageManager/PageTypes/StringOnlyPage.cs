using System;
using System.Linq;

namespace PageManager
{
    public interface IAllocateStringPage
    {
        StringOnlyPage AllocatePageStr(ulong prevPage, ulong nextPage);
        StringOnlyPage GetPageStr(ulong pageId);
    }

    public interface IPageWithOffsets<T>
    {
        public uint MergeWithOffsetFetch(T item);
        public T FetchWithOffset(uint offset);
    }

    public class StringOnlyPage : PageSerializerBase<char[][]>, IPageWithOffsets<char[]>
    {
        public StringOnlyPage(uint pageSize, ulong pageId, ulong prevPageId, ulong nextPageId)
        {
            if (pageSize < IPage.FirstElementPosition + sizeof(char) * 2)
            {
                throw new ArgumentException("Size can't be less than size of char and null termination");
            }

            this.pageSize = pageSize;
            this.pageId = pageId;
            this.prevPageId = prevPageId;

            this.content = new byte[pageSize];

            Serialize(new char[0][]);
        }

        public override char[][] Deserialize()
        {
            int numOfElements = BitConverter.ToInt32(this.content.AsSpan((int)IPage.NumOfRowsPosition, sizeof(int)));
            char[][] elementsToReturn = new char[numOfElements][];

            uint currentPositionInPage = IPage.FirstElementPosition;

            for (int elemPosition = 0; elemPosition < elementsToReturn.Length; elemPosition++)
            {
                // First iterate to find length.
                uint i = 0;
                while (this.content[currentPositionInPage + i] != 0x0) { i++; };

                elementsToReturn[elemPosition] = new char[i];

                i = 0;
                while (this.content[currentPositionInPage] != 0x0)
                {
                    elementsToReturn[elemPosition][i] = (char)this.content[currentPositionInPage];
                    currentPositionInPage++;
                    i++;
                };

                currentPositionInPage++;
            }

            return elementsToReturn;
        }

        public override PageType PageType() => PageManager.PageType.StringPage;

        public override uint GetSizeNeeded(char[][] items)
        {
            uint byteCount = 0;
            foreach (char[] item in items)
            {
                byteCount += (uint)item.Length + 1;
            }

            return byteCount;
        }

        protected override void SerializeInternal(char[][] items)
        {
            uint contentPosition = IPage.FirstElementPosition;

            foreach (char[] elem in items)
            {
                foreach (byte elemBytes in elem)
                {
                    content[contentPosition++] = elemBytes;
                }

                // Separate with null termination.
                content[contentPosition++] = (byte)'\0';
            }
        }

        public override uint MaxRowCount()
        {
            return this.pageSize - IPage.FirstElementPosition;
        }

        public override bool CanFit(char[][] items)
        {
            int size = 0;
            foreach (char[] item in items)
            {
                size += item.Length;
            }

            return this.pageSize - IPage.FirstElementPosition  >= size;
        }

        protected override uint GetRowCount(char[][] items)
        {
            return (uint)items.Length;
        }

        public override void Merge(char[][] items)
        {
            char[][] arr = Deserialize();
            arr = arr.Concat(items).ToArray();
            this.Serialize(arr);
        }

        public uint MergeWithOffsetFetch(char[] item)
        {
            int numOfElements = BitConverter.ToInt32(this.content.AsSpan((int)IPage.NumOfRowsPosition, sizeof(int)));

            int endCharFound = 0;
            uint i = IPage.FirstElementPosition;
            for (; i <  this.content.Length && endCharFound != numOfElements; i++)
            {
                if (content[i] == 0x0)
                {
                    endCharFound++;
                }
            }

            if (this.pageSize - i < item.Length + 1)
            {
                throw new NotEnoughSpaceException();
            }

            for (int j = 0; j < item.Length; j++)
            {
                this.content[i + j] = (byte)item[j];
            }

            this.content[item.Length] = 0x0;
            this.rowCount++;

            uint numofRowsPos = IPage.NumOfRowsPosition;
            foreach (byte numOfRowsByte in BitConverter.GetBytes(this.rowCount))
            {
                content[numofRowsPos++] = numOfRowsByte;
            }

            return i;
        }

        public char[] FetchWithOffset(uint offset)
        {
            if (offset < IPage.FirstElementPosition || offset >= this.pageSize)
            {
                throw new ArgumentException();
            }

            if (offset != IPage.FirstElementPosition && this.content[offset - 1] != 0x0)
            {
                throw new PageCorruptedException();
            }

            char[] result;

            uint length = 0;
            for (; this.content[offset + length] != 0x0; length++) { }
            result = new char[length];
            for (int i = 0; i < length; i++)
            {
                result[i] = (char)this.content[offset + i];
            }

            return result;
        }
    }
}
