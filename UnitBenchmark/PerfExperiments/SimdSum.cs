using System.Numerics;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using BenchmarkDotNet.Attributes;

namespace PerfExperiments
{
    public class SimdSum
    {
        [Params(100, 1000, 100_000, 1_000_000, 10_000_000)]
        public int ItemNum;

        public long[] ArrayItems;
        public int[] ArrayItemsInt;

        [IterationSetup]
        public void CreateArray()
        {
            this.ArrayItems = new long[this.ItemNum];
            this.ArrayItemsInt = new int[this.ItemNum];

            for (int i = 0; i < this.ArrayItems.Length; i++)
            {
                this.ArrayItems[i] = i * 2;
                this.ArrayItemsInt[i] = i * 2;
            }
        }

        [Benchmark]
        public void NaiveSum()
        {
            long result = 0;

            foreach (long i in this.ArrayItems)
            {
                result += i;
            }
        }

        [Benchmark]
        public void NaiveSumInt()
        {
            int result = 0;

            foreach (int i in this.ArrayItemsInt)
            {
                result += i;
            }
        }

        /*
        Removing this is just too slow.
        [Benchmark]
        public void LinqSum()
        {
            long sum = this.ArrayItems.Sum();
        }
        */

        [Benchmark]
        public long VectorSum()
        {
            int vectorSize = Vector<long>.Count;
            Vector<long> accVector = Vector<long>.Zero;

            for (int i = 0; i < this.ArrayItems.Length; i += vectorSize)
            {
                // TODO: This may require heap alloc.
                // something is wrong here since it allocates only first 2 elems?
                var v = new Vector<long>(this.ArrayItems, i);
                accVector += v;
            }

            long result = Vector.Dot(accVector, Vector<long>.One);
            return result;
        }

        [Benchmark]
        public unsafe long Intrinsics()
        {
            int vectorSize = 256 / 8 / 8;
            var accVector = Vector256<long>.Zero;

            fixed (long* ptr = this.ArrayItems)
            {
                for (int i = 0; i < this.ArrayItems.Length; i += vectorSize)
                {
                    Vector256<long> v = Avx2.LoadVector256(ptr + i);
                    accVector = Avx2.Add(accVector, v);
                }
            }

            long result = 0;
            var temp = stackalloc long[vectorSize];

            Avx2.Store(temp, accVector);

            for (int i = 0; i < vectorSize; i++)
            {
                result += temp[i];
            }

            return result;
        }

        [Benchmark]
        public unsafe long IntrinsicsInt()
        {
            int vectorSize = 256 / 8 / 4;
            var accVector = Vector256<int>.Zero;

            fixed (int* ptr = this.ArrayItemsInt)
            {
                for (int i = 0; i < this.ArrayItems.Length; i += vectorSize)
                {
                    Vector256<int> v = Avx2.LoadVector256(ptr + i);
                    accVector = Avx2.Add(accVector, v);
                }
            }

            int result = 0;
            var temp = stackalloc int[vectorSize];

            Avx2.Store(temp, accVector);

            for (int i = 0; i < vectorSize; i++)
            {
                result += temp[i];
            }

            return result;
        }
    }
}
