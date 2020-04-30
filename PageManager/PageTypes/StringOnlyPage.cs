using System;
using System.Runtime.Serialization;

namespace PageManager
{
    public interface IAllocateStringPage
    {
        StringOnlyPage AllocatePageStr();
        StringOnlyPage GetPageStr(ulong pageId);
    }

    public class StringOnlyPage : PageSerializerBase<char[][]>
    {
        public StringOnlyPage(uint pageSize, ulong pageId)
        {
            if (pageSize < IPage.FirstElementPosition + sizeof(char) * 2)
            {
                throw new ArgumentException("Size can't be less than size of char and null termination");
            }

            this.pageSize = pageSize;
            this.pageId = pageId;

            this.content = new byte[pageSize];

            Serialize(new char[0][]);
        }

        public override char[][] Deserialize()
        {
            int numOfElements = BitConverter.ToInt32(this.content.AsSpan((int)IPage.NumOfRowsPosition, sizeof(int)));
            char[][] elementsToReturn = new char[numOfElements][];

            uint currentPositionInPage = IPage.FirstElementPosition;

            for (int elemPosition = 0; elemPosition < elementsToReturn.Length; elemPosition++)
            {
                // First iterate to find length.
                uint i = 0;
                while (this.content[currentPositionInPage + i] != 0x0) { i++; };

                elementsToReturn[elemPosition] = new char[i];

                i = 0;
                while (this.content[currentPositionInPage] != 0x0)
                {
                    elementsToReturn[elemPosition][i] = (char)this.content[currentPositionInPage];
                    currentPositionInPage++;
                    i++;
                };

                currentPositionInPage++;
            }

            return elementsToReturn;
        }

        public override PageType PageType() => PageManager.PageType.StringPage;

        public override uint GetSizeNeeded(char[][] items)
        {
            uint byteCount = 0;
            foreach (char[] item in items)
            {
                byteCount += (uint)item.Length + 1;
            }

            return byteCount;
        }

        protected override void SerializeInternal(char[][] items)
        {
            uint contentPosition = IPage.FirstElementPosition;
            foreach (char[] elem in items)
            {
                foreach (byte elemBytes in elem)
                {
                    content[contentPosition++] = elemBytes;
                }

                // Separate with null termination.
                content[contentPosition++] = (byte)'\0';
            }
        }

        public override uint MaxRowCount()
        {
            return this.pageSize - IPage.FirstElementPosition;
        }

        public override bool CanFit(char[][] items)
        {
            int size = 0;
            foreach (char[] item in items)
            {
                size += item.Length;
            }

            return this.pageSize - IPage.FirstElementPosition  >= size;
        }

        protected override uint GetRowCount(char[][] items)
        {
            return (uint)items.Length;
        }
    }
}
