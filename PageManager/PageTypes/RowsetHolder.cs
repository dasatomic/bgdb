namespace PageManager
{
    public interface IRowsetHolder
    {
        public int[] GetIntColumn(int columnId);
        public double[] GetDoubleColumn(int columnId);
        public  PagePointerPair[] GetStringPointerColumn(int columnId);

        public void SetIntColumns(int[][] data);
        public void SetDoubleColumns(double[][] data);
        public void SetStringPointerColumns(PagePointerPair[][] data);
    }

    public class RowsetHolder : IRowsetHolder
    {
        private int[][] intColumns;
        private PagePointerPair[][] pagePointerColumns;
        private double[][] doubleColumns;
        private int[] columnIdToTypeIdMappers;

        public RowsetHolder(ColumnType[] columnTypes)
        {
            int intCount = 0;
            int doubleCount = 0;
            int pagePointerCount = 0;

            columnIdToTypeIdMappers = new int[columnTypes.Length];

            for (int i = 0; i < columnTypes.Length; i++)
            {
                switch (columnTypes[i])
                {
                    case ColumnType.Int:
                        columnIdToTypeIdMappers[i] = intCount;
                        intCount++;
                        break;
                    case ColumnType.Double:
                        columnIdToTypeIdMappers[i] = doubleCount;
                        doubleCount++;
                        break;
                    case ColumnType.StringPointer:
                        columnIdToTypeIdMappers[i] = pagePointerCount;
                        pagePointerCount++;
                        break;
                    default:
                        throw new UnexpectedEnumValueException<ColumnType>(columnTypes[i]);
                }
            }

            intColumns = new int[intCount][];
            pagePointerColumns = new PagePointerPair[pagePointerCount][];
            doubleColumns = new double[doubleCount][];
        }

        public int[] GetIntColumn(int columnId)
        {
            return intColumns[columnIdToTypeIdMappers[columnId]];
        }

        public void SetIntColumns(int[][] columns)
        {
            if (columns.Length != intColumns.Length)
            {
                throw new InvalidRowsetDefinitionException();
            }

            for (int i = 0; i < columns.Length; i++)
            {
                this.intColumns[i] = columns[i];
            }
        }

        public double[] GetDoubleColumn(int columnId)
        {
            return doubleColumns[columnIdToTypeIdMappers[columnId]];
        }

        public void SetDoubleColumns(double [][] columns)
        {
            if (columns.Length != doubleColumns.Length)
            {
                throw new InvalidRowsetDefinitionException();
            }

            for (int i = 0; i < columns.Length; i++)
            {
                this.doubleColumns[i] = columns[i];
            }
        }

        public PagePointerPair[] GetStringPointerColumn(int columnId)
        {
            return pagePointerColumns[columnIdToTypeIdMappers[columnId]];
        }

        public void SetStringPointerColumns(PagePointerPair[][] columns)
        {
            if (columns.Length != pagePointerColumns.Length)
            {
                throw new InvalidRowsetDefinitionException();
            }

            for (int i = 0; i < columns.Length; i++)
            {
                this.pagePointerColumns[i] = columns[i];
            }
        }
    }
}
