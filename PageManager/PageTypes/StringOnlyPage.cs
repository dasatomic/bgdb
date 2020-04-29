using System;
using System.Runtime.Serialization;

namespace PageManager
{
    public interface IAllocateStringPage
    {
        StringOnlyPage AllocatePageStr();
        StringOnlyPage GetPageStr(ulong pageId);
    }

    public class StringOnlyPage : IPageSerializer<char[][]>
    {
        private readonly uint pageSize;
        private readonly ulong pageId;

        // Byte representation:
        // [0-7] PageId
        // [8-11] PageSize
        // [12-15] PageType
        protected byte[] content;

        protected const uint PageIdPosition = 0;
        protected const uint PageSizePosition = 8;
        protected const uint PageTypePosition = 12;
        protected const uint NumOfRowsPosition = 16;
        protected const uint FirstElementPosition = 20;

        public StringOnlyPage(uint pageSize, ulong pageId)
        {
            if (pageSize < FirstElementPosition + sizeof(char) * 2)
            {
                throw new ArgumentException("Size can't be less than size of char and null termination");
            }

            this.pageSize = pageSize;
            this.pageId = pageId;

            this.content = new byte[pageSize];

            Serialize(new char[0][]);
        }

        public char[][] Deserialize()
        {
            int numOfElements = BitConverter.ToInt32(this.content.AsSpan((int)NumOfRowsPosition, sizeof(int)));
            char[][] elementsToReturn = new char[numOfElements][];

            uint currentPositionInPage = FirstElementPosition;

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

        public byte[] GetContent() => this.content;

        public ulong PageId() => this.pageId;

        public PageType PageType() => PageManager.PageType.StringPage;

        public uint SizeInBytes() => this.pageSize;

        private uint GetSizeNeeded(char[][] items)
        {
            uint byteCount = 0;
            foreach (char[] item in items)
            {
                byteCount += (uint)item.Length + 1;
            }

            return byteCount;
        }

        public void Serialize(char[][] items)
        {
            if (this.MaxRowCount() < this.GetSizeNeeded(items))
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

            foreach (byte typeByte in BitConverter.GetBytes((int)PageManager.PageType.StringPage))
            {
                content[contentPosition] = typeByte;
                contentPosition++;
            }

            foreach (byte numOfRowsByte in BitConverter.GetBytes(items.Length))
            {
                content[contentPosition] = numOfRowsByte;
                contentPosition++;
            }

            SerializeInternal(items);
        }

        private void SerializeInternal(char[][] items)
        {
            uint contentPosition = FirstElementPosition;
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

        public uint MaxRowCount()
        {
            return this.pageSize - FirstElementPosition;
        }

        public bool CanFit(char[][] items)
        {
            int size = 0;
            foreach (char[] item in items)
            {
                size += item.Length;
            }

            return this.pageSize - FirstElementPosition  >= size;
        }
    }
}
