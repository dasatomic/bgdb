using NUnit.Framework;
using PageManager;
using PageManager.PageTypes;
using Test.Common;

namespace PageManagerTests
{
    [TestFixture]
    public class BitTrackingPageTests
    {
        [Test]
        public void BitTrackingPageSet()
        {
            MixedPage page = new MixedPage(4096, 1, new [] { new ColumnInfo(ColumnType.Int) }, 0, 0, new byte[4096], 0, new DummyTran());

            for (int i = 0; i < page.MaxRowCount(); i++)
            {
                RowHolder rhf = new RowHolder(new ColumnType[] { ColumnType.Int });
                rhf.SetField<int>(0, 0);
                page.Insert(rhf, new DummyTran());
            }

            BitTrackingPage bPage = new BitTrackingPage(page);

            Assert.IsFalse(bPage.At(0, new DummyTran()));
            Assert.IsFalse(bPage.At(893, new DummyTran()));
            bPage.Set(893, new DummyTran());
            Assert.IsTrue(bPage.At(893, new DummyTran()));
        }

        [Test]
        public void BitTrackingPageSetUnsetRandom([Random(0, 4096, 1000)] int rowToSet)
        {
            MixedPage page = new MixedPage(4096, 1, new [] { new ColumnInfo(ColumnType.Int) }, 0, 0, new byte[4096], 0, new DummyTran());

            for (int i = 0; i < page.MaxRowCount(); i++)
            {
                RowHolder rhf = new RowHolder(new ColumnType[] { ColumnType.Int });
                rhf.SetField<int>(0, 0);
                page.Insert(rhf, new DummyTran());
            }

            BitTrackingPage bPage = new BitTrackingPage(page);

            Assert.IsFalse(bPage.At(rowToSet, new DummyTran()));
            bPage.Set(rowToSet, new DummyTran());
            Assert.IsTrue(bPage.At(rowToSet, new DummyTran()));
        }

        [Test]
        public void MaxSet()
        {
            MixedPage page = new MixedPage(4096, 1, new [] { new ColumnInfo(ColumnType.Int) }, 0, 0, new byte[4096 - IPage.FirstElementPosition], 0, new DummyTran());

            for (int i = 0; i < page.MaxRowCount(); i++)
            {
                RowHolder rhf = new RowHolder(new ColumnType[] { ColumnType.Int });
                rhf.SetField<int>(0, 0);
                Assert.AreEqual(i, page.Insert(rhf, new DummyTran()));
            }

            BitTrackingPage bPage = new BitTrackingPage(page);
            const int maxRowCount = 31392;

            Assert.AreEqual(maxRowCount, bPage.MaxItemCount());
            Assert.IsFalse(bPage.At(maxRowCount - 1, new DummyTran()));
            bPage.Set(maxRowCount - 1, new DummyTran());
            Assert.IsTrue(bPage.At(maxRowCount - 1, new DummyTran()));
        }
    }
}
