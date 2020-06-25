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

        private readonly ColumnType[] masterPageColumnDefinition = new ColumnType[]
        {
            // MD object id.
            ColumnType.Int,
            // Pointer to first page.
            ColumnType.PagePointer,
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
                using (ITransaction tran = new Transaction(this.logManager, this.pageAllocator, "SETUP_BOOT_PAGE"))
                using (Releaser releaser = tran.AcquireLock(IBootPageAllocator.BootPageId, LockManager.LockTypeEnum.Exclusive).Result)
                {
                    bootPageAllocator.AllocatePageBootPage(PageType.MixedPage, this.masterPageColumnDefinition, tran);
                    this.masterMetadataCollection = new PageListCollection(this.pageAllocator, this.masterPageColumnDefinition, this.pageAllocator.GetMixedPage(IBootPageAllocator.BootPageId, tran, this.masterPageColumnDefinition).Result);
                    tran.Commit();
                }

                MetadataInitialSetup();
            }
            else
            {
                using ITransaction tran = new Transaction(this.logManager, this.pageAllocator, "GET_BOOT_PAGE");
                using Releaser releaser = tran.AcquireLock(IBootPageAllocator.BootPageId, LockManager.LockTypeEnum.Exclusive).Result;
                this.masterMetadataCollection = new PageListCollection(this.pageAllocator, this.masterPageColumnDefinition, this.pageAllocator.GetMixedPage(IBootPageAllocator.BootPageId, tran, this.masterPageColumnDefinition).Result);
                tran.Commit();
            }
        }

        private void MetadataInitialSetup()
        {
            using ITransaction tran = new Transaction(this.logManager, this.pageAllocator, "MetadataSetup");
            RowsetHolder rh = new RowsetHolder(this.masterPageColumnDefinition);

            MixedPage mdColumnsFirstPage = this.pageAllocator.AllocateMixedPage(MetadataColumnsManager.GetSchemaDefinition(), PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran).Result;
            this.columnsManager = new MetadataColumnsManager(this.pageAllocator, mdColumnsFirstPage, this.stringHeap);

            MixedPage mdTableFirstPage = this.pageAllocator.AllocateMixedPage(MetadataTablesManager.GetSchemaDefinition(), PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran).Result;
            this.tableManager = new MetadataTablesManager(this.pageAllocator, mdTableFirstPage, this.stringHeap, this.columnsManager);

            rh.SetColumns(
                new int[1][] { new int[] 
                { 
                    (int)MetadataObjectEnum.MdTableId,
                    (int)MetadataObjectEnum.MdColumnId,
                }},
                new double[0][], new PagePointerOffsetPair[0][],
                new long[1][] { new long[] 
                { 
                    (long)mdTableFirstPage.PageId(),
                    (long)mdColumnsFirstPage.PageId() 
                }});
            masterMetadataCollection.Add(rh, tran).Wait();

            tran.Commit().Wait();
        }

        public MetadataTablesManager GetTableManager() => this.tableManager;

        public MetadataColumnsManager GetColumnManager() => this.columnsManager;
    }
}
