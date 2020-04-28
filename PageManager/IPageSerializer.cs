namespace PageManager
{
    public interface IPageSerializer<T>
    {
        public void Serialize(T items);
        public T Deserialize();
    }
}
