using System;

namespace PageManager
{
    public interface IAllocateIntegerPage
    {
        IntegerOnlyPage AllocatePageInt();
        IntegerOnlyPage GetPageInt(ulong pageId);
    }

    public class IntegerOnlyPage : SimpleTypeOnlyPage<int>
    {
        public IntegerOnlyPage(uint pageSize, ulong pageId) : base(pageSize, pageId, PageManager.PageType.IntPage) { }

        public override int[] Deserialize()
        {
            int numOfElements = BitConverter.ToInt32(this.content.AsSpan((int)NumOfRowsPosition, sizeof(int)));
            int[] elements = new int[numOfElements];

            for (int i = 0; i < elements.Length; i++)
            {
                elements[i] = BitConverter.ToInt32(this.content.AsSpan((int)FirstElementPosition + i * sizeof(int), sizeof(int)));
            }

            return elements;
        }

        protected override void SerializeInternal(int[] items)
        {
            uint contentPosition = FirstElementPosition;
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
