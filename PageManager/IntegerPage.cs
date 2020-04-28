using System;

namespace PageManager
{
    public class IntegerOnlyPage : IPage, IPageSerializer<int[]>
    {
        private readonly uint pageSize;
        private readonly ulong pageId;

        // Byte representation:
        // [0-7] PageId
        // [8-11] PageSize
        // [12-15] PageType
        private byte[] content;

        private const uint PageIdPosition = 0;
        private const uint PageSizePosition = 8;
        private const uint PageTypePosition = 12;
        private const uint NumOfRowsPosition = 16;
        private const uint FirstElementPosition = 20;

        public IntegerOnlyPage(uint pageSize, ulong pageId)
        {
            if (pageSize < FirstElementPosition + sizeof(int))
            {
                throw new ArgumentException("Size can't be less than size of int");
            }

            if (pageSize % sizeof(int) != 0)
            {
                throw new ArgumentException("Page size needs to be divisible with elem type");
            }

            this.pageSize = pageSize;
            this.pageId = pageId;

            this.content = new byte[pageSize];

            Serialize(new int[0]);
        }

        public int[] Deserialize()
        {
            int numOfElements = BitConverter.ToInt32(this.content.AsSpan((int)NumOfRowsPosition, sizeof(int)));
            int[] elements = new int[numOfElements];

            for (int i = 0; i < elements.Length; i++)
            {
                elements[i] = BitConverter.ToInt32(this.content.AsSpan((int)FirstElementPosition + i * sizeof(int), sizeof(int)));
            }

            return elements;
        }

        public byte[] GetContent() => this.content;

        public ulong PageId() => this.pageId;

        public PageType PageType() => PageManager.PageType.IntPage;

        public void Serialize(int[] items)
        {
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

            foreach (byte typeByte in BitConverter.GetBytes((int)PageManager.PageType.IntPage))
            {
                content[contentPosition] = typeByte;
                contentPosition++;
            }

            foreach (byte numOfRowsByte in BitConverter.GetBytes(items.Length))
            {
                content[contentPosition] = numOfRowsByte;
                contentPosition++;
            }

            foreach (int elem in items)
            {
                foreach (byte elemBytes in BitConverter.GetBytes(elem))
                {
                    content[contentPosition] = elemBytes;
                    contentPosition++;
                }
            }
        }

        public uint SizeInBytes()
        {
            return this.pageSize;
        }
    }
}
