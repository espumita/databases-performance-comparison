using BenchmarkDotNet.Running;
using SqlServer;

namespace databases_performance_comparison;

public class Program {
    public static async Task Main(string[] args) {
        // --- AzureCosmosDB
        //var azureCosmosDbBenchmarks = new AzureCosmosDBBenchmarks();
        //await azureCosmosDbBenchmarks.Setup();
        //var summary = BenchmarkRunner.Run<AzureCosmosDBBenchmarks>();

        // -- SqlServer
        var sqlServerBenchmarks = new SqlServerBenchmarks();
        await sqlServerBenchmarks.Setup();
    }

}