using AzureCosmosDB;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using databases_performance_comparison.Model;
using System.Runtime.CompilerServices;

namespace databases_performance_comparison;

public class Program {
    public static async Task Main(string[] args) {
        var summary = BenchmarkRunner.Run<AzureCosmosDBBenchmarks>();
    }

}

[IterationCount(1)]
[WarmupCount(1)]
[MemoryDiagnoser]
public class AzureCosmosDBBenchmarks {
    private AzureCosmosDbConnection azureCosmosDbConnection;
    private List<SampleItem> sampleItems;

    [Params(1000)] public int NumberOfValues { get; set; }

    [GlobalSetup]
    public async Task Setup() {
        azureCosmosDbConnection = new AzureCosmosDbConnection();
        await azureCosmosDbConnection.SetDatabaseAndContainers();
        sampleItems = await SampleItems();
        await azureCosmosDbConnection.InsetItemsIfNotExists(sampleItems);
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
    public async Task Read() {
        var randomIndex = new Random()
            .Next(0, NumberOfValues);
        var item = await azureCosmosDbConnection.Query<SampleItem>(@$"
            SELECT 
                *
            FROM 
                items i
            WHERE 
                i.id = ""{sampleItems[randomIndex].id}""
        ");
        if (!item.Single().Equals(sampleItems[randomIndex])) throw new Exception("Read has fail!");
    }
}