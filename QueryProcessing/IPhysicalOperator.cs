namespace QueryProcessing
{
    public interface IPhysicalOperator<T>
    {
        void Invoke(T input);
    }
}
