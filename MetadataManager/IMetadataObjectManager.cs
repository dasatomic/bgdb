using PageManager;

namespace MetadataManager
{
    public interface IMetadataObjectManager
    {
        ColumnType[] GetSchemaDefinition();
    }
}
