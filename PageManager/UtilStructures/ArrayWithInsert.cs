using System;

namespace DataStructures
{
    public static class ArrayWithInsert
    {
        public static T[] RemoveAt<T>(this T[] items, int position)
        {
            T[] newArray = new T[items.Length - 1];

            if (position < 0 || position > items.Length - 1)
            {
                throw new IndexOutOfRangeException();
            }

            for (int i = 0, j = 0; i < newArray.Length; i++, j++)
            {
                if (i == position)
                {
                    j++;
                }

                newArray[i] = items[j];
            }

            return newArray;
        }
    }
}
