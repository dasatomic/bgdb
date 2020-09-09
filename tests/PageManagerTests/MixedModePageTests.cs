using NUnit.Framework;
using PageManager;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Test.Common;

namespace PageManagerTests
{
    class MixedModePageTests
    {
        private const int DefaultSize = 4096;
        private const int DefaultPageId = 42;
        private const int DefaultPrevPage = 41;
        private const int DefaultNextPage = 43;

        [Test]
        public void Insert()
        {
            var rows = GenerateDataUtils.GenerateRowsWithSampleData(out ColumnType[] types);
            MixedPage page = new MixedPage(DefaultSize, DefaultPageId, types, DefaultPrevPage, DefaultNextPage, new DummyTran());

            rows.ForEach(r => page.Insert(r, new DummyTran()));

            Assert.AreEqual(rows.ToArray(), page.Fetch(TestGlobals.DummyTran).ToArray());
        }

        [Test]
        public void VerifyFromStream()
        {
            var rows = GenerateDataUtils.GenerateRowsWithSampleData(out ColumnType[] types);
            MixedPage page = new MixedPage(DefaultSize, DefaultPageId, types, DefaultPrevPage, DefaultNextPage, new DummyTran());

            rows.ForEach(r => page.Insert(r, new DummyTran()));

            byte[] content = new byte[DefaultSize];

            using (var stream = new MemoryStream(content))
            using (var bw = new BinaryWriter(stream))
            {
                page.Persist(bw);
            }

            var source = new BinaryReader(new MemoryStream(content));
            MixedPage pageDeserialized = new MixedPage(source, types);

            Assert.AreEqual(page.PageId(), pageDeserialized.PageId());
            Assert.AreEqual(page.PageType(), pageDeserialized.PageType());
            Assert.AreEqual(page.RowCount(), pageDeserialized.RowCount());
            Assert.AreEqual(page.NextPageId(), pageDeserialized.NextPageId());
            Assert.AreEqual(page.PrevPageId(), pageDeserialized.PrevPageId());

            var result = pageDeserialized.Fetch(TestGlobals.DummyTran);
            Assert.AreEqual(rows.ToArray(), result.ToArray());
        }
    }
}
