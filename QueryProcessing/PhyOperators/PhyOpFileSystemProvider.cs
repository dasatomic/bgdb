using MetadataManager;
using PageManager;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class PhyOpFileSystemProvider : IPhysicalOperator<RowHolder>
    {
        private readonly string sourcePath;
        const int MaxPathLength = 256;
        const int ExtensionLength = 4;

        public PhyOpFileSystemProvider(string sourcePath)
        {
            this.sourcePath = sourcePath;
        }

        public MetadataColumn[] GetOutputColumns()
        {
            return new MetadataColumn[]
            {
                new MetadataColumn(0, 0, "FilePath", new ColumnInfo(ColumnType.String, MaxPathLength)),
                new MetadataColumn(1, 0, "FileName", new ColumnInfo(ColumnType.String, MaxPathLength)),
                new MetadataColumn(2, 0, "Extension", new ColumnInfo(ColumnType.String, ExtensionLength)),
                new MetadataColumn(3, 0, "FileSize", new ColumnInfo(ColumnType.Int)),
            };
        }

        public async IAsyncEnumerable<RowHolder> Iterate(ITransaction tran)
        {
            EnumerationOptions options = new EnumerationOptions()
            {
                IgnoreInaccessible = true,
                RecurseSubdirectories = false,
            };

            const string searchPattern = "*.*";

            foreach (string file in Directory.EnumerateFiles(this.sourcePath, searchPattern, options))
            {
                string fullName = Path.GetFullPath(file);
                string fileName = Path.GetFileName(fullName);
                string extension = Path.GetExtension(fullName);

                if (fullName.Length > MaxPathLength)
                {
                    throw new NotImplementedException("Currently path limit is 256 chars");
                }

                // TODO: Need support for longs.
                long length = new FileInfo(fullName).Length;

                if (length > int.MaxValue)
                {
                    throw new NotImplementedException("Currently there is no support for longs");
                }

                RowHolder rh = new RowHolder(new ColumnInfo[] 
                {
                    new ColumnInfo(ColumnType.String, MaxPathLength),
                    new ColumnInfo(ColumnType.String, MaxPathLength),
                    new ColumnInfo(ColumnType.String, ExtensionLength),
                    new ColumnInfo(ColumnType.Int),
                });

                rh.SetField(0, fullName.ToCharArray());
                rh.SetField(1, fileName.ToCharArray());
                rh.SetField(2, extension.ToCharArray());
                rh.SetField<int>(3, (int)length);

                yield return await Task.FromResult(rh);
            }
        }
    }
}
