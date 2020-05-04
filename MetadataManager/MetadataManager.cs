using PageManager;
using System;

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

        public MetadataManager(IAllocateMixedPage pageAllocator, HeapWithOffsets<char[]> stringHeap, bool useExistingMasterPage)
        {
            this.pageAllocator = pageAllocator;
            this.stringHeap = stringHeap;

            if (!useExistingMasterPage)
            {
                this.masterMetadataCollection = new PageListCollection(this.pageAllocator, this.masterPageColumnDefinition);
                MetadataInitialSetup();
            }
            else
            {
                this.masterMetadataCollection = new PageListCollection(this.pageAllocator, this.masterPageColumnDefinition, this.pageAllocator.GetMixedPage(0));
            }
        }

        private void MetadataInitialSetup()
        {
            var mdTableFirstPage = this.pageAllocator.AllocateMixedPage(MetadataTablesManager.GetSchemaDefinition(), 0, 0);
            this.tableManager = new MetadataTablesManager(this.pageAllocator, mdTableFirstPage, this.stringHeap);

            var mdColumnsFirstPage = this.pageAllocator.AllocateMixedPage(MetadataColumnsManager.GetSchemaDefinition(), 0, 0);
            this.columnsManager = new MetadataColumnsManager(this.pageAllocator, mdColumnsFirstPage, this.stringHeap);
        }
    }
}
