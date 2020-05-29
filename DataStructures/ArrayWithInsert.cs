namespace DataStructures
{
    public static class ArrayWithInsert
    {
        public static T[] RemoveAt<T>(this T[] items, int position)
        {
            T[] newArray = new T[items.Length - 1];

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
