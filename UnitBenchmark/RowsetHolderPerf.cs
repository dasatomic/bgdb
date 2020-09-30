using BenchmarkDotNet.Attributes;
using PageManager;
using System;

namespace UnitBenchmark
{
    [RPlotExporter]
    public class RowsetHolderPerf
    {
        [Params(100000, 500000, 1000000, 10000000)]
        public int IterNum;

        [Benchmark]
        public void RowsetHolderFixedTest()
        {
            var columnTypes = new ColumnInfo[] 
            { 
                new ColumnInfo(ColumnType.Int),
                new ColumnInfo(ColumnType.Double),
                new ColumnInfo(ColumnType.StringPointer),
                new ColumnInfo(ColumnType.String, 20)
            };

            RowHolder rh = new RowHolder(columnTypes);
            rh.SetField<int>(0, 1);
            rh.SetField<double>(1, 3.1);
            rh.SetField(2, new PagePointerOffsetPair(5, 5));
            rh.SetField(3, "TESTTESTTEST".ToCharArray());

            Memory<byte> memory = new Memory<byte>(new byte[4096]);

            for (int i = 0; i < IterNum / 10; i++)
            {
                RowsetHolder rs = new RowsetHolder(columnTypes, memory, true);

                for (int j = 0; j < 10; j++)
                {
                    rs.InsertRow(rh);
                }
            }
        }
    }
}
