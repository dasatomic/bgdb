using System.Numerics;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace PerfExperiments
{
    /// <summary>
    /// L1 data on 1600x is 6x32kb (32kb per core).
    /// to have optimal L1 hit we can host ~8k ints
    /// </summary>
    [SimpleJob(runtimeMoniker: RuntimeMoniker.Net60)]
    public class SimdSum
    {
        [Params(8_000, 10_000, 100_000, 1_000_000, 10_000_000)]
        public int ItemNum;

        public long[] ArrayItemsLong;
        public int[] ArrayItemsInt;

        [IterationSetup]
        public void CreateArray()
        {
            this.ArrayItemsLong = new long[this.ItemNum];
            this.ArrayItemsInt = new int[this.ItemNum];

            for (int i = 0; i < this.ArrayItemsLong.Length; i++)
            {
                this.ArrayItemsLong[i] = i * 2;
                this.ArrayItemsInt[i] = i * 2;
            }
        }

        [Benchmark]
        public void NaiveSumLong()
        {
            long result = 0;

            foreach (long i in this.ArrayItemsLong)
            {
                result += i;
            }
        }

        [Benchmark(Baseline = true)]
        public void NaiveSumInt()
        {
            int result = 0;

            foreach (int i in this.ArrayItemsInt)
            {
                result += i;
            }
        }

        [Benchmark]
        public long LinqSumLong()
        {
            return this.ArrayItemsLong.Sum();
        }

        [Benchmark]
        public long LinqSumInt()
        {
            return this.ArrayItemsInt.Sum();
        }

        private T VectorSum<T>(T[] data) where T : struct
        {
            int vectorSize = Vector<T>.Count;
            Vector<T> accVector = Vector<T>.Zero;

            for (int i = 0; i < data.Length; i += vectorSize)
            {
                var v = new Vector<T>(data, i);
                accVector += v;
            }

            T result = Vector.Dot(accVector, Vector<T>.One);
            return result;
        }

        [Benchmark]
        public long VectorSumLong()
        {
            return VectorSum<long>(this.ArrayItemsLong);
        }

        [Benchmark]
        public long VectorSumInt()
        {
            return VectorSum<int>(this.ArrayItemsInt);
        }


        [Benchmark]
        public unsafe long IntrinsicsSumLong()
        {
            int vectorSize = Vector256<long>.Count;
            var accVector = Vector256<long>.Zero;

            fixed (long* ptr = this.ArrayItemsLong)
            {
                for (int i = 0; i < this.ArrayItemsLong.Length; i += vectorSize)
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
        public unsafe int IntrinsicsInt()
        {
            int vectorSize = Vector256<int>.Count;
            var accVector = Vector256<int>.Zero;

            fixed (int* ptr = this.ArrayItemsInt)
            {
                for (int i = 0; i < this.ArrayItemsInt.Length; i += vectorSize)
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

        [Benchmark]
        public unsafe int IntrinsicsIntAligned()
        {
            int vectorSize = Vector256<int>.Count;
            var accVector = Vector256<int>.Zero;

            fixed (int* ptr = this.ArrayItemsInt)
            {
                // It may skip first couple of elements. Don't care about this.
                int* aligned = (int*)(((ulong)ptr + 31UL) & ~31UL);
                var pos = (int)(aligned - ptr);
                for (int i = pos; i < this.ArrayItemsInt.Length; i += vectorSize)
                {
                    Vector256<int> v = Avx2.LoadAlignedVector256(ptr + i);
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

        [Benchmark]
        public unsafe long IntrinsicsIntLoopUnfold()
        {
            int vectorSize = Vector256<int>.Count;
            var accVector = Vector256<int>.Zero;

            fixed (int* ptr = this.ArrayItemsInt)
            {
                int* aligned = (int*)(((ulong)ptr + 31UL) & ~31UL);
                var pos = (int)(aligned - ptr);

                int loopUnfold = 8 * vectorSize;
                const int loopUnfold1 = 8;
                const int loopUnfold2 = 16;
                const int loopUnfold3 = 24;
                const int loopUnfold4 = 32;
                const int loopUnfold5 = 40;
                const int loopUnfold6 = 48;
                const int loopUnfold7 = 56;
                for (int i = pos; i < this.ArrayItemsInt.Length; i += loopUnfold)
                {
                    Vector256<int> v = Avx2.LoadAlignedVector256(ptr + i);
                    accVector = Avx2.Add(accVector, v);
                    v = Avx2.LoadAlignedVector256(ptr + i + loopUnfold1);
                    accVector = Avx2.Add(accVector, v);
                    v = Avx2.LoadAlignedVector256(ptr + i + loopUnfold2);
                    accVector = Avx2.Add(accVector, v);
                    v = Avx2.LoadAlignedVector256(ptr + i + loopUnfold3);
                    accVector = Avx2.Add(accVector, v);
                    v = Avx2.LoadAlignedVector256(ptr + i + loopUnfold4);
                    accVector = Avx2.Add(accVector, v);
                    v = Avx2.LoadAlignedVector256(ptr + i + loopUnfold5);
                    accVector = Avx2.Add(accVector, v);
                    v = Avx2.LoadAlignedVector256(ptr + i + loopUnfold6);
                    accVector = Avx2.Add(accVector, v);
                    v = Avx2.LoadAlignedVector256(ptr + i + loopUnfold7);
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
