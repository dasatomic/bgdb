using System;
using System.Collections.Generic;
using System.Text;

namespace MetadataManager
{
    public enum MetadataObjectEnum
    {
        MdColumnId = 1,
        MdTableId = 2,
    }

    public struct MetadataIdPageIdPair
    {
        public MetadataObjectEnum MedataObjectId;
        public ulong FirstPageId;

        public MetadataIdPageIdPair(MetadataObjectEnum metadataObjectId, ulong firstPageId)
        {
            this.MedataObjectId = metadataObjectId;
            this.FirstPageId = firstPageId;
        }
    }
}
