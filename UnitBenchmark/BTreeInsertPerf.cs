using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnostics.Windows.Configs;
using DataStructures;
using PageManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Test.Common;

namespace UnitBenchmark
{
    [RPlotExporter]
    //[EtwProfiler(performExtraBenchmarksRun: true)]
    public class BTreeInsertPerf
    {
        [Params(50_000, 100_000, 200_000 /* 500_000, 1_000_000 */)]
        public int RowsInTableNumber;

        public enum GenerationStrategy
        {
            Seq,
            Rev,
            Rand,
            FromFile,
        }

        List<RowHolder> itemsToInsertRand;

        ColumnInfo[] schema = new ColumnInfo[]
        {
            new ColumnInfo(ColumnType.Int)
        };

        private List<int> GenerateItems(GenerationStrategy strat, int itemNum)
        {
            switch (strat)
            {
                case GenerationStrategy.Seq:
                    return Enumerable.Range(0, itemNum).ToList();
                case GenerationStrategy.Rev:
                    return Enumerable.Range(0, itemNum).Reverse().ToList();
                case GenerationStrategy.Rand:
                    Random rnd = new Random();
                    return Enumerable.Range(0, itemNum).OrderBy(x => rnd.Next()).Distinct().ToList();
                default:
                    throw new ArgumentException();
            }
        }

        [GlobalSetup]
        public void Setup()
        {
            itemsToInsertRand = GenerateItems(GenerationStrategy.Rand, this.RowsInTableNumber).Select(item =>
            {
                var row = new RowHolder(schema);
                row.SetField(0, item);
                return row;
            }).ToList();
        }

        [Benchmark]
        public async Task InsertIntoBTreeSingleIntColumnRandomData()
        {
            var pageManager =  new PageManager.PageManager(4096, new FifoEvictionPolicy(10000, 5), TestGlobals.DefaultPersistedStream);
            using ITransaction tran = new DummyTran();

            Func<RowHolder, RowHolder, int> comp = (rh1, rh2) =>
                rh1.GetField<int>(0).CompareTo(rh2.GetField<int>(0));

            BTreeCollection collection =
                new BTreeCollection(pageManager, this.schema, new DummyTran(), comp, 0);

            foreach (var item in this.itemsToInsertRand)
            {
                await collection.Add(item, tran);
            }
        }
    }
}
