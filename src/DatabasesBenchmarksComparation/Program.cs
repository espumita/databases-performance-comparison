using AzureCosmosDB;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using System.Runtime.CompilerServices;
using AzureCosmosDB.Model;
using Microsoft.Azure.Cosmos;

namespace databases_performance_comparison;

public class Program {
    public static async Task Main(string[] args) {
        var summary = BenchmarkRunner.Run<AzureCosmosDBBenchmarks>();
    }

}

[IterationCount(10)]
[WarmupCount(1)]
[MemoryDiagnoser]
public class AzureCosmosDBBenchmarks {
    private AzureCosmosDbConnection azureCosmosDbConnection;
    private List<SampleItem> sampleItems;
    private List<Option4SampleItem> option4SamplesItems;

    [Params(1000)] public int NumberOfValues { get; set; }

    [GlobalSetup]
    public async Task Setup() {
        azureCosmosDbConnection = new AzureCosmosDbConnection();
        await azureCosmosDbConnection.SetDatabaseAndContainers();
        sampleItems = await SampleItems();
        await azureCosmosDbConnection.InsetOption3AItemsIfNotExists(sampleItems);
        await azureCosmosDbConnection.InsetOption3BItemsIfNotExists(sampleItems);
        option4SamplesItems = sampleItems.Select(x => new Option4SampleItem(
            $"{x.id}-{x.TenantId}-{x.UserId}-{x.SessionId}",
            x.Data
        )).ToList();
        await azureCosmosDbConnection.InsetOption4ItemsIfNotExists(option4SamplesItems);
    }

    static string WhereAmI([CallerFilePath] string callerFilePath = "") => callerFilePath;

    private async Task<List<SampleItem>> SampleItems() {
        const string sampleItemsCsv = "./cosmos-db-sample-items.csv";
        string basePath = Path.GetDirectoryName(WhereAmI());
        string targetPath = Path.Combine(basePath, sampleItemsCsv);
        if (File.Exists(targetPath)) {
            var lines = await File.ReadAllLinesAsync(targetPath);
            return lines.Select(CreateSampleItemFromLine).ToList();
        }
        var items = Enumerable.Range(0, NumberOfValues)
            .Select(x => ASampleItemWithRandomIds())
            .ToList();
        await File.WriteAllLinesAsync(targetPath, items.Select(x => $"{x.id},{x.TenantId},{x.UserId},{x.SessionId},{x.Data}"));
        return items;
    }

    private static SampleItem CreateSampleItemFromLine(string line) {
        var args = line.Split(',');
        return new SampleItem(args[0], args[1], args[2], args[3], args[4]);
    }

    private static SampleItem ASampleItemWithRandomIds() {
        return new SampleItem(
            Guid.NewGuid().ToString(),
            Guid.NewGuid().ToString(),
            Guid.NewGuid().ToString(),
            Guid.NewGuid().ToString(),
            Guid.NewGuid().ToString()
        );
    }

    [Benchmark]
    public async Task ReadOption3A() {
        var randomIndex = new Random()
            .Next(0, NumberOfValues);
        var item = await azureCosmosDbConnection.Query<SampleItem>(@$"
            SELECT 
                *
            FROM 
                items i
            WHERE 
                i.id = '{sampleItems[randomIndex].id}'
                AND i.TenantId = '{sampleItems[randomIndex].TenantId}'
                AND i.UserId = '{sampleItems[randomIndex].UserId}'
                AND i.SessionId = '{sampleItems[randomIndex].SessionId}'
        ", 
        AzureCosmosDbConnection.Container3AId);
        if (!item.Single().Equals(sampleItems[randomIndex])) throw new Exception("Read has fail!");
    }

    [Benchmark]
    public async Task ReadOption3B() {
        var randomIndex = new Random()
            .Next(0, NumberOfValues);
        var item = await azureCosmosDbConnection.Query<SampleItem>(@$"
            SELECT 
                *
            FROM 
                items i
            WHERE 
                i.id = '{sampleItems[randomIndex].id}'
                AND i.TenantId = '{sampleItems[randomIndex].TenantId}'
                AND i.UserId = '{sampleItems[randomIndex].UserId}'
                AND i.SessionId = '{sampleItems[randomIndex].SessionId}'
        ",
        AzureCosmosDbConnection.Container3BId,
        new QueryRequestOptions {
            PartitionKey = new PartitionKey(sampleItems[randomIndex].id)
        });
        if (!item.Single().Equals(sampleItems[randomIndex])) throw new Exception("Read has fail!");
    }

    [Benchmark]
    public async Task ReadOption4() {
        var randomIndex = new Random()
            .Next(0, NumberOfValues);
        var item = await azureCosmosDbConnection.Query<Option4SampleItem>(@$"
            SELECT 
                *
            FROM 
                items i
            WHERE 
                i.id = '{option4SamplesItems[randomIndex].id}'
            ",
            AzureCosmosDbConnection.Container4Id,
            new QueryRequestOptions {
                PartitionKey = new PartitionKey(option4SamplesItems[randomIndex].id)
            });
        if (!item.Single().Equals(option4SamplesItems[randomIndex])) throw new Exception("Read has fail!");
    }
}