using System;
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

            this.content = new byte[pageSize];

            Serialize(new T[0]);
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

        protected override uint GetRowCount(T[] items)
        {
            return (uint)items.Length;
        }

        public override void Merge(T[] items)
        {
            T[] arr = this.Deserialize();
            arr = arr.Concat(items).ToArray();
            this.Serialize(arr);
        }
    }
}
