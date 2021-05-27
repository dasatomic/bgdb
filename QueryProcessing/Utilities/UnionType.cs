using System;

namespace QueryProcessing.Utilities
{
    /// <summary>
    /// Abstract class used as base for Union types.
    /// Use it when you want to encompass two different classes
    /// into single one, with common projection.
    /// </summary>
    /// <typeparam name="A">Type A</typeparam>
    /// <typeparam name="B">Type B</typeparam>
    public abstract class Union2Type<A, B>
    {
        public abstract T Match<T>(Func<A, T> f, Func<B, T> g);

        private Union2Type() { }

        public sealed class Case1 : Union2Type<A, B>
        {
            public readonly A Item;
            public Case1(A item): base()
            {
                this.Item = item;
            }

            public override T Match<T>(Func<A, T> f, Func<B, T> g)
            {
                return f(this.Item);
            }
        }

        public sealed class Case2: Union2Type<A, B>
        {
            public readonly B Item;
            public Case2(B item): base()
            {
                this.Item = item;
            }

            public override T Match<T>(Func<A, T> f, Func<B, T> g)
            {
                return g(this.Item);
            }
        }
    }
}
