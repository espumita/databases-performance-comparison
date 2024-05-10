using System.Runtime.CompilerServices;
using AzureCosmosDB;
using AzureCosmosDB.Model;
using BenchmarkDotNet.Attributes;
using Microsoft.Azure.Cosmos;

namespace databases_performance_comparison;

[IterationCount(100)]
[WarmupCount(1)]
[MemoryDiagnoser]
public class AzureCosmosDBBenchmarks {
    private AzureCosmosDbConnection azureCosmosDbConnection;
    private List<SampleItem> sampleItems;
    private List<Option2SampleItem> option2SamplesItems;
    private List<Option3SampleItem> option3SamplesItems;

    [Params(100000)] public int NumberOfValues { get; set; } = 10000;

    [GlobalSetup]
    public async Task Setup() {
        azureCosmosDbConnection = new AzureCosmosDbConnection();
        await azureCosmosDbConnection.SetUpDatabaseAndContainers();
        sampleItems = await SampleItems();
        await azureCosmosDbConnection.InsetOption1ItemsIfNotExists(sampleItems);
        option2SamplesItems = sampleItems.Select(x => new Option2SampleItem(
            x.id,
            $"{x.TenantId}-{x.UserId}-{x.SessionId}",
            x.Data
        )).ToList();
        option3SamplesItems = sampleItems.Select(x => new Option3SampleItem(
            $"{x.id}-{x.TenantId}-{x.UserId}-{x.SessionId}",
            x.Data
        )).ToList();
        await azureCosmosDbConnection.InsetOption2ItemsIfNotExists(option2SamplesItems);
        await azureCosmosDbConnection.InsetOption3ItemsIfNotExists(option3SamplesItems);
        await azureCosmosDbConnection.InsetOption4ItemsIfNotExists(sampleItems);
    }

    [Benchmark]
    public async Task ReadOption1() {
        var randomIndex = new Random()
            .Next(0, NumberOfValues);
        var item = await azureCosmosDbConnection.Query<SampleItem>(@$"
            SELECT
                c.id,
                t.TenantId,
                u.UserId,
                s.SessionId,
                s.Data
            FROM 
                c
            JOIN
                 t IN c.tenants
            JOIN
                 u IN t.users
            JOIN
                 s IN u.sessions
            WHERE
                c.id = '{sampleItems[randomIndex].id}'
                AND t.TenantId = '{sampleItems[randomIndex].TenantId}'
                AND u.UserId = '{sampleItems[randomIndex].UserId}'
                AND s.SessionId = '{sampleItems[randomIndex].SessionId}'
        ", 
            AzureCosmosDbConnection.Container1Id,
            new QueryRequestOptions {
                PartitionKey = new PartitionKey(sampleItems[randomIndex].id)
            });
        if (!item.Single().Equals(sampleItems[randomIndex])) throw new Exception("Read has fail!");
    }

    [Benchmark]
    public async Task ReadOption2() {
        var randomIndex = new Random()
            .Next(0, NumberOfValues);
        var item = await azureCosmosDbConnection.Query<Option2SampleItem>(@$"
            SELECT
                c.id,
                t.TenantUserAndSessionId,
                t.Data
            FROM 
                c
            JOIN
                 t IN c.Rows
            WHERE
                c.id = '{option2SamplesItems[randomIndex].id}'
                AND t.TenantUserAndSessionId = '{option2SamplesItems[randomIndex].TenantUserAndSessionId}'
        ",
            AzureCosmosDbConnection.Container2Id,
            new QueryRequestOptions {
                PartitionKey = new PartitionKey(option2SamplesItems[randomIndex].id)
            });
        if (!item.Single().Equals(option2SamplesItems[randomIndex])) throw new Exception("Read has fail!");
    }

    [Benchmark]
    public async Task ReadOption3() {
        var randomIndex = new Random()
            .Next(0, NumberOfValues);
        var item = await azureCosmosDbConnection.ReadItemAsync<Option3SampleItem>(
            option3SamplesItems[randomIndex].id,
            AzureCosmosDbConnection.Container3Id,
            new PartitionKey(option3SamplesItems[randomIndex].id)
        );
        if (!item.Equals(option3SamplesItems[randomIndex])) throw new Exception("Read has fail!");
    }

    [Benchmark]
    public async Task ReadOption4() {
        var randomIndex = new Random()
            .Next(0, NumberOfValues);
        var item = await azureCosmosDbConnection.ReadItemAsync<SampleItem>(
            sampleItems[randomIndex].id,
            AzureCosmosDbConnection.Container4Id,
            new PartitionKeyBuilder()
                .Add(sampleItems[randomIndex].TenantId)
                .Add(sampleItems[randomIndex].UserId)
                .Add(sampleItems[randomIndex].SessionId)
                .Build());
        if (!item.Equals(sampleItems[randomIndex])) throw new Exception("Read has fail!");
    }

    private async Task<List<SampleItem>> SampleItems() {
        const string sampleItemsCsv = "./cosmos-db-sample-items.csv";
        string basePath = Path.GetDirectoryName(WhereAmI());
        string targetPath = Path.Combine(basePath, sampleItemsCsv);
        if (File.Exists(targetPath)) {
            var lines = await File.ReadAllLinesAsync(targetPath);
            return lines.Select(CreateSampleItemFromLine).ToList();
        }
        var ids = Enumerable.Range(0, NumberOfValues / 1000).Select(x => Guid.NewGuid().ToString()).ToList();
        var tenantsIds = Enumerable.Range(0, 10).Select(x => Guid.NewGuid().ToString()).ToList();
        var usersIds = Enumerable.Range(0, 10).Select(x => Guid.NewGuid().ToString()).ToList();
        var sessionIds = Enumerable.Range(0, 10).Select(x => Guid.NewGuid().ToString()).ToList();

        var items = new List<SampleItem>();
        ids.ForEach(id => {
            tenantsIds.ForEach(tenantId => {
                usersIds.ForEach(userId => {
                    sessionIds.ForEach(sessionId => {
                        items.Add(new SampleItem(
                            id,
                            tenantId,
                            userId,
                            sessionId,
                            Guid.NewGuid().ToString())
                        );
                    });
                });
            });
        });

        await File.WriteAllLinesAsync(targetPath, items.Select(x => $"{x.id},{x.TenantId},{x.UserId},{x.SessionId},{x.Data}"));
        return items;
    }

    static string WhereAmI([CallerFilePath] string callerFilePath = "") => callerFilePath;

    private static SampleItem CreateSampleItemFromLine(string line) {
        var args = line.Split(',');
        return new SampleItem(args[0], args[1], args[2], args[3], args[4]);
    }
}