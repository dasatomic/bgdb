﻿using System;

namespace PageManager
{
    public interface IAllocateDoublePage
    {
        DoubleOnlyPage AllocatePageDouble();
        DoubleOnlyPage GetPageDouble(ulong pageId);
    }

    public class DoubleOnlyPage : SimpleTypeOnlyPage<double>
    {
        public DoubleOnlyPage(uint pageSize, ulong pageId) : base(pageSize, pageId, PageManager.PageType.DoublePage) { }

        public override double[] Deserialize()
        {
            int numOfElements = BitConverter.ToInt32(this.content.AsSpan((int)NumOfRowsPosition, sizeof(int)));
            double[] elements = new double[numOfElements];

            for (int i = 0; i < elements.Length; i++)
            {
                elements[i] = BitConverter.ToDouble(this.content.AsSpan((int)FirstElementPosition + i * sizeof(double), sizeof(double)));
            }

            return elements;
        }

        protected override void SerializeInternal(double[] items)
        {
            uint contentPosition = FirstElementPosition;
            foreach (double elem in items)
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