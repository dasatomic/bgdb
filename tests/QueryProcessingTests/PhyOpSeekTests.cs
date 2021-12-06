using DataStructures;
using LogManager;
using MetadataManager;
using NUnit.Framework;
using PageManager;
using QueryProcessing;
using QueryProcessing.PhyOperators;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Test.Common;

namespace QueryProcessingTests
{
    public class PhyOpSeekTests
    {
        [Test]
        [TestCase(0)]
        [TestCase(2)]
        public async Task ValidateSeek(int clusteredIndexPosition)
        {
            var allocator =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            ILogManager logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            ITransaction setupTran = logManager.CreateTransaction(allocator);
            StringHeapCollection stringHeap = new StringHeapCollection(allocator, setupTran);
            MetadataManager.MetadataManager mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);

            var tm = mm.GetTableManager();


            ITransaction tran = logManager.CreateTransaction(allocator);
            var columnInfos = new[] { new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.String, 1), new ColumnInfo(ColumnType.Double) };
            int id = await tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "Table",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = columnInfos, 
                ClusteredIndexPositions = new int[] { clusteredIndexPosition },
            }, tran);

            await tran.Commit();

            tran = logManager.CreateTransaction(allocator);
            var table = await tm.GetById(id, tran);
            await tran.Commit();

            List<RowHolder> source = new List<RowHolder>();
            for (int i = 0; i < 5; i++)
            {
                var rhf = new RowHolder(new[] { new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.String, 1), new ColumnInfo(ColumnType.Double) });
                rhf.SetField<int>(0, i);
                rhf.SetField(1, i.ToString().ToCharArray());
                rhf.SetField<double>(2, i + 1.1);
                source.Add(rhf);
            }

            PhyOpStaticRowProvider opStatic = new PhyOpStaticRowProvider(source);

            tran = logManager.CreateTransaction(allocator);
            PhyOpTableInsert op = new PhyOpTableInsert(table.Collection, opStatic);
            await op.Iterate(tran).AllResultsAsync();
            await tran.Commit();

            tran = logManager.CreateTransaction(allocator);
            List<RowHolder> seekValues = new List<RowHolder>();

            var rowSeekSchema = new ColumnInfo[1];

            if (clusteredIndexPosition == 0)
            {
                rowSeekSchema[0] = new ColumnInfo(ColumnType.Int);
            }
            else
            {
                rowSeekSchema[0] = new ColumnInfo(ColumnType.Double);
            }

            for (int i = 0; i < 5; i++)
            {
                RowHolder rh = new RowHolder(rowSeekSchema);
                if (clusteredIndexPosition == 0)
                {
                    rh.SetField<int>(0, i);
                }
                else
                {
                    rh.SetField<double>(0, i + 1.1);
                }

                seekValues.Add(rh);
            }

            PhyOpSeek seek = null;
            if (clusteredIndexPosition == 0)
            {
                seek = new PhyOpSeek(table.Collection, tran, table.Columns, "Table", seekValues, ColumnType.Int);
            }
            else
            {
                seek = new PhyOpSeek(table.Collection, tran, table.Columns, "Table", seekValues, ColumnType.Double);
            }


            List<RowHolder> result = new List<RowHolder>();

            await foreach (var row in seek.Iterate(tran))
            {
                result.Add(row);
            }

            Assert.AreEqual(source, result.ToArray());

        }
    }
}
