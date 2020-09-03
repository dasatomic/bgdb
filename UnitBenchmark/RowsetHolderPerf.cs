using BenchmarkDotNet.Attributes;
using PageManager;
using System;

namespace UnitBenchmark
{
    public class RowsetHolderPerf
    {
        [Params(100000, 200000, 300000, 400000, 500000, 1000000)]
        public int IterNum;

        [Benchmark]
        public void RowsetHolderFixedTest()
        {
            var columnTypes = new ColumnType[] { ColumnType.Int, ColumnType.Int, ColumnType.Double, ColumnType.StringPointer };

            RowHolderFixed rh = new RowHolderFixed(columnTypes);
            rh.SetField<int>(0, 1);
            rh.SetField<int>(1, 2);
            rh.SetField<double>(2, 3.1);
            rh.SetField(3, new PagePointerOffsetPair(5, 5));

            Memory<byte> memory = new Memory<byte>(new byte[4096]);

            for (int i = 0; i < IterNum; i++)
            {
                RowsetHolderFixed rs = new RowsetHolderFixed(columnTypes, memory, true);

                for (int j = 0; j < 10; j++)
                {
                    rs.InsertRow(rh);
                }
            }
        }
    }
}
