using AzureCosmosDB.Model;
using BenchmarkDotNet.Attributes;
using SqlServer;
using SqlServer.Model;
using System.Runtime.CompilerServices;

namespace databases_performance_comparison;

[IterationCount(10)]
[WarmupCount(1)]
[MemoryDiagnoser]
public class SqlServerBenchmarks {
    private SqlServerConnection sqlServerConnection;
    private List<ProductOperation> productOperations;

    [Params(80000)] public int NumberOfValues { get; set; } = 8000;

    [GlobalSetup]
    public async Task Setup() {
        sqlServerConnection = new SqlServerConnection();
        await sqlServerConnection.SetUpDatabaseAndTables();
        productOperations = await ProductOperations();
    }

    private async Task<List<ProductOperation>> ProductOperations() {
        const string productOperationsCsv = "./mssql-product-operations.csv";
        string basePath = Path.GetDirectoryName(WhereAmI());
        string targetPath = Path.Combine(basePath, productOperationsCsv);
        if (File.Exists(targetPath)) {
            var lines = await File.ReadAllLinesAsync(targetPath);
            return lines.Select(CreateProductOperationFromLine).ToList();
        }
        var operations = Enumerable.Range(0, NumberOfValues)
            .Select(x => new ProductOperation(
                Id: Guid.NewGuid().ToString(),
                Status: OperationStatus.InProgress, //TODO randomize
                ProductId: Guid.NewGuid().ToString(),
                StartDate: DateTime.Now,
                EndDate: DateTime.Now,
                Details: Guid.NewGuid().ToString()
            ))
            .ToList();
        await File.WriteAllLinesAsync(targetPath, operations.Select(x => $"{x.Id},{x.Status},{x.ProductId},{x.StartDate},{x.EndDate},{x.Details}"));
        return operations;
    }

    static string WhereAmI([CallerFilePath] string callerFilePath = "") => callerFilePath;

    private static ProductOperation CreateProductOperationFromLine(string line) {
        var args = line.Split(',');// todo
        return new ProductOperation(args[0], OperationStatus.InProgress, args[2], DateTime.Now, DateTime.Now, args[5]);
    }
}
