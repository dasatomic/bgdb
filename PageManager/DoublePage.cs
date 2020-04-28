using System;
using System.Runtime.Serialization;

namespace PageManager
{
    public interface IAllocateDoublePage
    {
        DoubleOnlyPage AllocatePageDouble();
        DoubleOnlyPage GetPageDouble(ulong pageId);
    }

    public class DoubleOnlyPage : IPageSerializer<double[]>
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

        public DoubleOnlyPage(uint pageSize, ulong pageId)
        {
            if (pageSize < FirstElementPosition + sizeof(double))
            {
                throw new ArgumentException("Size can't be less than size of int");
            }

            if (pageSize % sizeof(double) != 0)
            {
                throw new ArgumentException("Page size needs to be divisible with elem type");
            }

            this.pageSize = pageSize;
            this.pageId = pageId;

            this.content = new byte[pageSize];

            Serialize(new double[0]);
        }

        public double[] Deserialize()
        {
            int numOfElements = BitConverter.ToInt32(this.content.AsSpan((int)NumOfRowsPosition, sizeof(int)));
            double[] elements = new double[numOfElements];

            for (int i = 0; i < elements.Length; i++)
            {
                elements[i] = BitConverter.ToDouble(this.content.AsSpan((int)FirstElementPosition + i * sizeof(double), sizeof(double)));
            }

            return elements;
        }

        public byte[] GetContent() => this.content;

        public ulong PageId() => this.pageId;

        public PageType PageType() => PageManager.PageType.DoublePage;

        public void Serialize(double[] items)
        {
            if (this.MaxRowCount() < items.Length)
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

            foreach (double elem in items)
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

        public uint MaxRowCount()
        {
            return (this.pageSize - FirstElementPosition) / sizeof(double);
        }
    }
}
