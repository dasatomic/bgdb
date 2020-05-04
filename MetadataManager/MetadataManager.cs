using PageManager;
using System;
using System.Collections.Generic;

namespace MetadataManager
{
    public class MetadataManager
    {
        private IAllocateMixedPage pageAllocator;

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

        public MetadataManager(IAllocateMixedPage pageAllocator, HeapWithOffsets<char[]> stringHeap, IBootPageAllocator bootPageAllocator)
        {
            this.pageAllocator = pageAllocator;
            this.stringHeap = stringHeap;

            if (!bootPageAllocator.BootPageInitialized())
            {
                bootPageAllocator.AllocatePageBootPage(PageType.MixedPage, this.masterPageColumnDefinition);
                this.masterMetadataCollection = new PageListCollection(this.pageAllocator, this.masterPageColumnDefinition, this.pageAllocator.GetMixedPage(IBootPageAllocator.BootPageId));
                MetadataInitialSetup();
            }
            else
            {
                this.masterMetadataCollection = new PageListCollection(this.pageAllocator, this.masterPageColumnDefinition, this.pageAllocator.GetMixedPage(IBootPageAllocator.BootPageId));
            }
        }

        private void MetadataInitialSetup()
        {
            RowsetHolder rh = new RowsetHolder(this.masterPageColumnDefinition);

            var mdColumnsFirstPage = this.pageAllocator.AllocateMixedPage(MetadataColumnsManager.GetSchemaDefinition(), 0, 0);
            this.columnsManager = new MetadataColumnsManager(this.pageAllocator, mdColumnsFirstPage, this.stringHeap);

            var mdTableFirstPage = this.pageAllocator.AllocateMixedPage(MetadataTablesManager.GetSchemaDefinition(), 0, 0);
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
            masterMetadataCollection.Add(rh);
        }

        public MetadataTablesManager GetTableManager() => this.tableManager;

        public MetadataColumnsManager GetColumnManager() => this.columnsManager;
    }
}
