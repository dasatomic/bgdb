using System;
using System.IO;

namespace PageManager
{
    public interface IAllocateDoublePage
    {
        DoubleOnlyPage AllocatePageDouble(ulong prevPage, ulong nextPage);
        DoubleOnlyPage GetPageDouble(ulong pageId);
    }

    public class DoubleOnlyPage : SimpleTypeOnlyPage<double>
    {
        public DoubleOnlyPage(uint pageSize, ulong pageId, ulong prevPageId, ulong nextPageId) : base(pageSize, pageId, PageManager.PageType.DoublePage, prevPageId, nextPageId) { }
        public DoubleOnlyPage(BinaryReader stream) : base(stream, PageManager.PageType.DoublePage) { }

        protected override void SerializeInternal(BinaryReader stream)
        {
            this.items = new double[this.rowCount];

            for (int i = 0; i < this.items.Length; i++)
            {
                this.items[i] = stream.ReadDouble();
            }
        }

        public override void Persist(Stream destination)
        {
            throw new NotImplementedException();
        }
    }
}
