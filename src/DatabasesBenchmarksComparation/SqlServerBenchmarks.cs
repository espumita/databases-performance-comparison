using System.Globalization;
using BenchmarkDotNet.Attributes;
using SqlServer;
using SqlServer.Model;
using System.Runtime.CompilerServices;
using SqlServer.Migrations;

namespace databases_performance_comparison;

[IterationCount(10)]
[WarmupCount(1)]
[MemoryDiagnoser]
public class SqlServerBenchmarks {
    private SqlServerConnection sqlServerConnection;
    private List<ProductOperation> productOperations;

    [Params(80000)] public int NumberOfValues { get; set; } = 80000;

    [GlobalSetup]
    public async Task Setup() {
        sqlServerConnection = new SqlServerConnection();
        await sqlServerConnection.SetUpDatabaseAndTables();
        productOperations = await ProductOperations();
        await sqlServerConnection.InsetProductOperationsIfNotExists(productOperations);
    }

    [Benchmark]
    public async Task BaseCase() {
        var randomIndex = new Random()
            .Next(0, NumberOfValues);
        var operations = await sqlServerConnection.QueryById(@$"
          SELECT
                OperationId,
                OperationStatus,
                ProductId,
                OperationStartDate,
                OperationEndDate,
                OperationDetails
          FROM
              dbo.{DB.ProductsOperationsTable}
          WHERE
              OperationId = @OperationId",
            "@OperationId", productOperations[randomIndex].Id);
        if (!operations.Single().Equals(productOperations[randomIndex])) throw new Exception("Read has fail!");
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
            .Select(x => ProductOperation())
            .ToList();
        await File.WriteAllLinesAsync(targetPath, operations.Select(x => $"{x.Id},{(int)x.Status},{x.ProductId},{x.StartDate.ToString("yyyyMMdd_hhmmss", CultureInfo.InvariantCulture)},{x.EndDate.ToString("yyyyMMdd_hhmmss", CultureInfo.InvariantCulture)},{x.Details}"));
        return operations;
    }

    private ProductOperation ProductOperation() {
        return new ProductOperation(
            Id: Guid.NewGuid().ToString(),
            Status: (OperationStatus) new Random().Next(0, 4),
            ProductId: Guid.NewGuid().ToString(),
            StartDate: RandomDate(),
            EndDate: RandomDate(),
            Details: Guid.NewGuid().ToString()
        );
    }

    private DateTime RandomDate() {
        var random = new Random();
        var year = random.Next(1995, DateTime.UtcNow.Year);
        var month = random.Next(1, 13);
        var day = random.Next(1, 28);
        var hour = random.Next(1, 24);
        var minutes = random.Next(1, 60);
        var second = random.Next(1, 60);
        return new DateTime(year, month, day, hour, minutes, second);
    }

    static string WhereAmI([CallerFilePath] string callerFilePath = "") => callerFilePath;

    private static ProductOperation CreateProductOperationFromLine(string line) {
        var args = line.Split(',');
        return new ProductOperation(
            args[0],
            (OperationStatus) int.Parse(args[1]),
            args[2],
            DateTime.ParseExact(args[3], "yyyyMMdd_hhmmss", CultureInfo.InvariantCulture),
            DateTime.ParseExact(args[4], "yyyyMMdd_hhmmss", CultureInfo.InvariantCulture),
            args[5]);
    }
}
