using MetadataManager;

namespace QueryProcessing
{
    internal class OrderByColumn
    {
        public enum Direction : int { Asc = 1, Desc = -1}

        public readonly MetadataColumn column;
        public readonly Direction direction;

        public OrderByColumn(MetadataColumn column, Direction direction)
        {
            this.column = column;
            this.direction = direction;
        }
    }
}