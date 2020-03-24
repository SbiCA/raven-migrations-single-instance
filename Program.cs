using System;
using System.Reflection;
using System.Threading;
using Microsoft.Extensions.Logging;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Migrations;

namespace MigrationTests
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            var docStore = new DocumentStore
            {
                Urls = new[] {"http://localhost:8080"},
                Database = "Test"
            }.Initialize();
            var options = new MigrationOptions();
            options.Assemblies.Add(Assembly.GetExecutingAssembly());
            var runner = new SingleInstanceRunner(docStore, options, loggerFactory.CreateLogger<MigrationRunner>());

            runner.Run();
        }
    }

    public class SingleInstanceRunner : MigrationRunner
    {
        private readonly ILogger<MigrationRunner> _logger;
        private readonly MigrationOptions _options;
        private readonly IDocumentStore _store;

        public SingleInstanceRunner(IDocumentStore store, MigrationOptions options, ILogger<MigrationRunner> logger)
            : base(store, options, logger)
        {
            _store = store;
            _options = options;
            _logger = logger;
        }

        public new void Run()
        {
            LockUsingCompareExchange();
            // LockUsingOptimisticConcurrency();
        }

        private void LockUsingCompareExchange()
        {
            // At this point, the user document has an Id assigned
            long? lockAcquired = null;
            using var session = _store.OpenSession();
            try
            {
                var result = _store.Operations.Send(
                    new PutCompareExchangeValueOperation<string>("LockMigrations", "locked", 0));

                if (result.Successful == false)
                {
                    _logger.LogWarning(
                        "Could not acquire lock ... already running migration or got cancelled without proper shutdown");
                    return;
                }

                lockAcquired = result.Index;
                _logger.LogInformation("acquired migration lock");
                base.Run();
            }
            finally
            {
                if (lockAcquired is {})
                {
                    var unlocked = _store.Operations.Send(
                        new DeleteCompareExchangeValueOperation<string>("LockMigrations", lockAcquired.Value));
                    if (unlocked.Successful == false)
                    {
                        _logger.LogError("Could not release lock, this need manual intervention");
                    }
                    else
                    {
                        _logger.LogInformation("released migration lock");
                    }
                }
            }
        }

        private void LockUsingOptimisticConcurrency()
        {
            try
            {
                using var session = _store.OpenSession();
                _store.Maintenance.Send(new ConfigureExpirationOperation(new ExpirationConfiguration
                {
                    Disabled = false,
                    DeleteFrequencyInSec = 60
                }));

                var expiry = DateTime.UtcNow.AddMinutes(10);
                session.Advanced.UseOptimisticConcurrency = true;
                var lockDocument = new LockDocument();
                session.Store(lockDocument);
                session.Advanced.GetMetadataFor(lockDocument)[Constants.Documents.Metadata.Expires] = expiry;
                session.Advanced.WaitForReplicationAfterSaveChanges(majority: true);
                session.SaveChanges();

                try
                {
                    // acquired lock document
                    _logger.LogInformation("acquired migration lock and running ...");
                    base.Run();
                }
                finally
                {
                    session.Delete(lockDocument.Id);
                    session.SaveChanges();
                }
            }
            catch (ConcurrencyException)
            {
                _logger.LogWarning(
                    "Could not acquire lock ... already running migration or got cancelled without proper shutdown");
            }
        }

        public class LockDocument
        {
            public readonly string Id = nameof(LockDocument);
            public DateTime LockedAt = DateTime.UtcNow;
        }
    }

    [Migration(1)]
    public class MyMigration : Migration
    {
        public override void Up()
        {
            using var session = DocumentStore.OpenSession();
            var doc = new
            {
                Id = string.Empty,
                Name = "test"
            };
            session.Store(doc);
            session.SaveChanges();
            Thread.Sleep(20_000);
        }
    }
}