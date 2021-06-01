using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public static class TaskExtension
    {
        public static async Task<ICollection<T>> AllResultsAsync<T>(this IAsyncEnumerable<T> asyncEnumerable)
        {
            if (null == asyncEnumerable)
            {
                throw new ArgumentNullException(nameof(asyncEnumerable));
            }

            var list = new List<T>();
            await foreach (var t in asyncEnumerable)
            {
                list.Add(t);
            }

            return list;
        }

        public static IAsyncEnumerator<T> EmptyEnumerator<T>() => EmptyAsyncEnumerator<T>.Instance;
        public static IAsyncEnumerable<T> EmptyEnumerable<T>() => EmptyAsyncEnumerable<T>.Instance;

        class EmptyAsyncEnumerator<T> : IAsyncEnumerator<T>
        {
            public static readonly EmptyAsyncEnumerator<T> Instance =
                new EmptyAsyncEnumerator<T>();
            public T Current => default!;
            public ValueTask DisposeAsync() => default;
            public ValueTask<bool> MoveNextAsync() => new ValueTask<bool>(false);
        }

        class EmptyAsyncEnumerable<T> : IAsyncEnumerable<T>
        {
            public static readonly EmptyAsyncEnumerable<T> Instance =
                new EmptyAsyncEnumerable<T>();

            public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
            {
                return TaskExtension.EmptyEnumerator<T>();
            }
        }
    }
}
