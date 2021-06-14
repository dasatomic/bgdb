using DataStructures;
using LogManager;
using PageManager;
using QueryProcessing;
using System.IO;
using System.Threading.Tasks;
using Test.Common;
using VideoProcessing;

namespace E2EQueryExecutionTests
{
    public abstract class BaseTestSetup 
    {
        protected QueryEntryGate queryEntryGate;
        protected ILogManager logManager;
        protected IPageManager pageManager;
        protected MetadataManager.MetadataManager metadataManager;

        private static string GetTempFolderPath()
        {
            FileInfo dataRoot = new FileInfo(typeof(BaseTestSetup).Assembly.Location);
            string assemblyFolderPath = dataRoot.Directory.FullName;

            string path = Path.Combine(assemblyFolderPath, "temp");
            Directory.CreateDirectory(path);
            return path;
        }

        protected virtual async Task Setup()
        {
            this.pageManager =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            this.logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            StringHeapCollection stringHeap = null;

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "SETUP"))
            {
                stringHeap = new StringHeapCollection(pageManager, tran);
                await tran.Commit();
            }

            metadataManager = new MetadataManager.MetadataManager(pageManager, stringHeap, pageManager, logManager);

            var videoChunker = new FfmpegVideoChunker(GetTempFolderPath(), TestGlobals.TestFileLogger);
            var videoChunkerCallback = SourceRegistration.VideoChunkerCallback(videoChunker);
            AstToOpTreeBuilder treeBuilder = new AstToOpTreeBuilder(metadataManager, videoChunkerCallback);

            this.queryEntryGate = new QueryEntryGate(
                statementHandlers: new ISqlStatement[]
                {
                    new CreateTableStatement(metadataManager),
                    new InsertIntoTableStatement(treeBuilder),
                    new SelectStatement(treeBuilder),
                });
        }
    }
}
