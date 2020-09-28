using DataStructures;
using LogManager;
using NUnit.Framework;
using PageManager;
using PageManager.Exceptions;
using QueryProcessing;
using QueryProcessing.Exceptions;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Test.Common;

namespace E2EQueryExecutionTests
{
    public abstract class BaseTestSetup 
    {
        protected QueryEntryGate queryEntryGate;
        protected ILogManager logManager;
        protected IPageManager pageManager;
        protected MetadataManager.MetadataManager metadataManager;

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
            AstToOpTreeBuilder treeBuilder = new AstToOpTreeBuilder(metadataManager);

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
