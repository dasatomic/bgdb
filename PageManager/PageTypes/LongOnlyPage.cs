using System;

namespace PageManager
{
    public interface IAllocateLongPage
    {
        LongOnlyPage AllocatePageLong();
        LongOnlyPage GetPageLong(ulong pageId);
    }

    public class LongOnlyPage : SimpleTypeOnlyPage<long>
    {
        public LongOnlyPage(uint pageSize, ulong pageId) : base(pageSize, pageId, PageManager.PageType.LongPage) { }

        public override long[] Deserialize()
        {
            int numOfElements = BitConverter.ToInt32(this.content.AsSpan((int)IPage.NumOfRowsPosition, sizeof(int)));
            long[] elements = new long[numOfElements];

            for (int i = 0; i < elements.Length; i++)
            {
                elements[i] = BitConverter.ToInt64(this.content.AsSpan((int)IPage.FirstElementPosition + i * sizeof(long), sizeof(long)));
            }

            return elements;
        }

        protected override void SerializeInternal(long[] items)
        {
            uint contentPosition = IPage.FirstElementPosition;

            foreach (long elem in items)
            {
                foreach (byte elemBytes in BitConverter.GetBytes(elem))
                {
                    content[contentPosition] = elemBytes;
                    contentPosition++;
                }
            }
        }
    }
}
