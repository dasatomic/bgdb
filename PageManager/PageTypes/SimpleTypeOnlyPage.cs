﻿using System;
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

        public SimpleTypeOnlyPage(uint pageSize, ulong pageId, PageType pageType, ulong prevPageId, ulong nextPageId)
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

            Store(new T[0]);
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
            return this.pageSize - IPage.FirstElementPosition - this.FooterLenght() >= (uint)Marshal.SizeOf(default(T)) * items.Length;
        }

        public override uint GetSizeNeeded(T[] items)
        {
            return (uint)items.Length * (uint)Marshal.SizeOf(default(T));
        }

        public override void Merge(T[] items)
        {
            this.items = this.items.Concat(items).ToArray();
        }

        public override T[] Fetch()
        {
            return this.items;
        }
        public override void Store(T[] items)
        {
            uint neededSize = this.GetSizeNeeded(items);
            if (neededSize > this.pageSize - IPage.FirstElementPosition)
            {
                throw new SerializationException();
            }

            this.items = items;
            this.rowCount = (uint)items.Length;
        }


        protected abstract void SerializeInternal(BinaryReader stream);
    }
}
