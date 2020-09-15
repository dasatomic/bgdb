using DataStructures;
using LockManager.LockImplementation;
using LogManager;
using PageManager;
using System;
using System.Collections.Generic;

namespace MetadataManager
{
    public class MetadataManager
    {
        private IPageManager pageAllocator;

        private readonly ColumnInfo[] masterPageColumnDefinition = new ColumnInfo[]
        {
            // MD object id.
            new ColumnInfo(ColumnType.Int),
            // Pointer to first page.
            new ColumnInfo(ColumnType.PagePointer),
        };

        private PageListCollection masterMetadataCollection;
        private HeapWithOffsets<char[]> stringHeap;

        private MetadataColumnsManager columnsManager;
        private MetadataTablesManager tableManager;

        private LogManager.ILogManager logManager;

        public MetadataManager(IPageManager pageAllocator, HeapWithOffsets<char[]> stringHeap, IBootPageAllocator bootPageAllocator, ILogManager logManager)
        {
            this.pageAllocator = pageAllocator;
            this.stringHeap = stringHeap;
            this.logManager = logManager;

            if (!bootPageAllocator.BootPageInitialized())
            {
                using (ITransaction tran = this.logManager.CreateTransaction(this.pageAllocator, false, "GET_BOOT_PAGE"))
                using (Releaser releaser = tran.AcquireLock(IBootPageAllocator.BootPageId, LockManager.LockTypeEnum.Exclusive).Result)
                {
                    bootPageAllocator.AllocatePageBootPage(PageType.MixedPage, this.masterPageColumnDefinition, tran);
                    this.masterMetadataCollection = new PageListCollection(this.pageAllocator, this.masterPageColumnDefinition, IBootPageAllocator.BootPageId);
                    tran.Commit();
                }

                MetadataInitialSetup();
            }
            else
            {
                using (ITransaction tran = this.logManager.CreateTransaction(this.pageAllocator, false, "GET_BOOT_PAGE"))
                {
                    using Releaser releaser = tran.AcquireLock(IBootPageAllocator.BootPageId, LockManager.LockTypeEnum.Exclusive).Result;
                    this.masterMetadataCollection = new PageListCollection(this.pageAllocator, this.masterPageColumnDefinition, IBootPageAllocator.BootPageId);
                    tran.Commit();
                }
            }
        }

        private void MetadataInitialSetup()
        {
            using ITransaction tran = this.logManager.CreateTransaction(this.pageAllocator, false, "MetadataSetup");

            MixedPage mdColumnsFirstPage = this.pageAllocator.AllocateMixedPage(MetadataColumnsManager.GetSchemaDefinition(), PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran).Result;
            this.columnsManager = new MetadataColumnsManager(this.pageAllocator, mdColumnsFirstPage, this.stringHeap);

            MixedPage mdTableFirstPage = this.pageAllocator.AllocateMixedPage(MetadataTablesManager.GetSchemaDefinition(), PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran).Result;
            this.tableManager = new MetadataTablesManager(this.pageAllocator, mdTableFirstPage, this.stringHeap, this.columnsManager);

            RowHolderFixed rhf = new RowHolderFixed(this.masterPageColumnDefinition);
            rhf.SetField<int>(0, (int)MetadataObjectEnum.MdTableId);
            rhf.SetField<long>(1, (long)mdTableFirstPage.PageId());

            masterMetadataCollection.Add(rhf, tran).Wait();
            rhf.SetField<int>(0, (int)MetadataObjectEnum.MdColumnId);
            rhf.SetField<long>(1, (long)mdColumnsFirstPage.PageId());
            masterMetadataCollection.Add(rhf, tran).Wait();

            tran.Commit().Wait();
        }

        public MetadataTablesManager GetTableManager() => this.tableManager;

        public MetadataColumnsManager GetColumnManager() => this.columnsManager;
    }
}
