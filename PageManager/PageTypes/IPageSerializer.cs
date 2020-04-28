namespace PageManager
{
    public interface IPageSerializer<T> : IPage
    {
        public void Serialize(T items);
        public T Deserialize();
        public uint MaxRowCount();
    }
}
