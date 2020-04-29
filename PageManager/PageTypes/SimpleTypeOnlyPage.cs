using System;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;

namespace PageManager
{
    public abstract class SimpleTypeOnlyPage<T> : IPageSerializer<T[]>
        where T : struct
    {
        private readonly uint pageSize;
        private readonly ulong pageId;
        private readonly PageType pageType;

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

        protected virtual uint FooterLenght() => 0;

        public SimpleTypeOnlyPage(uint pageSize, ulong pageId, PageType pageType)
        {
            if (pageSize < FirstElementPosition + (uint)Marshal.SizeOf(default(T)))
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

            this.content = new byte[pageSize];

            Serialize(new T[0]);
        }


        public byte[] GetContent() => this.content;

        public ulong PageId() => this.pageId;

        public PageType PageType() => this.pageType;

        public void Serialize(T[] items)
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

            foreach (byte typeByte in BitConverter.GetBytes((int)this.pageType))
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

        protected abstract void SerializeInternal(T[] items);

        public uint SizeInBytes()
        {
            return this.pageSize;
        }

        public uint MaxRowCount()
        {
            return (this.pageSize - FirstElementPosition - this.FooterLenght()) / (uint)Marshal.SizeOf(default(T));
        }

        public abstract T[] Deserialize();

        public bool CanFit(T[] items)
        {
            return this.pageSize - FirstElementPosition - this.FooterLenght() >= (uint)Marshal.SizeOf(default(T));
        }
    }
}
