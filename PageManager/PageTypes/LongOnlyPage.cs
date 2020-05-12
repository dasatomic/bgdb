using System;
using System.IO;

namespace PageManager
{
    public interface IAllocateLongPage
    {
        LongOnlyPage AllocatePageLong(ulong prevPage, ulong nextPage);
        LongOnlyPage GetPageLong(ulong pageId);
    }

    public class LongOnlyPage : SimpleTypeOnlyPage<long>
    {
        public LongOnlyPage(uint pageSize, ulong pageId, ulong prevPageId, ulong nextPageId) : base(pageSize, pageId, PageManager.PageType.LongPage, prevPageId, nextPageId) { }
        public LongOnlyPage(BinaryReader stream) : base(stream, PageManager.PageType.LongPage) { }

        protected override void SerializeInternal(BinaryReader stream)
        {
            this.items = new long[this.rowCount];

            for (int i = 0; i < this.items.Length; i++)
            {
                this.items[i] = stream.ReadInt64();
            }
        }

        public override void Persist(Stream destination)
        {
            using (BinaryWriter bw = new BinaryWriter(destination))
            {
                bw.Write(this.pageId);
                bw.Write(this.pageSize);
                bw.Write((int)this.PageType());
                bw.Write(this.rowCount);
                bw.Write(this.prevPageId);
                bw.Write(this.nextPageId);

                for (int i = 0; i < this.rowCount; i++)
                {
                    bw.Write(this.items[i]);
                }
            }
        }
    }
}
