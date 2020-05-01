using System;

namespace PageManager
{
    public interface IAllocateIntegerPage
    {
        IntegerOnlyPage AllocatePageInt(ulong prevPage, ulong nextPage);
        IntegerOnlyPage GetPageInt(ulong pageId);
    }

    public class IntegerOnlyPage : SimpleTypeOnlyPage<int>
    {
        public IntegerOnlyPage(uint pageSize, ulong pageId, ulong prevPageId, ulong nextPageId) : base(pageSize, pageId, PageManager.PageType.IntPage, prevPageId, nextPageId) { }

        public override int[] Deserialize()
        {
            int numOfElements = BitConverter.ToInt32(this.content.AsSpan((int)IPage.NumOfRowsPosition, sizeof(int)));
            int[] elements = new int[numOfElements];

            for (int i = 0; i < elements.Length; i++)
            {
                elements[i] = BitConverter.ToInt32(this.content.AsSpan((int)IPage.FirstElementPosition + i * sizeof(int), sizeof(int)));
            }

            return elements;
        }

        protected override void SerializeInternal(int[] items)
        {
            uint contentPosition = IPage.FirstElementPosition;

            foreach (int elem in items)
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
