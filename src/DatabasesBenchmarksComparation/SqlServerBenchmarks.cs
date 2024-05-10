using BenchmarkDotNet.Attributes;
using SqlServer;
using SqlServer.Model;
using System.Runtime.CompilerServices;
using SqlServer.Migrations;
using System.Linq;

namespace databases_performance_comparison;

[IterationCount(1)]
[WarmupCount(1)]
[MemoryDiagnoser]
public class SqlServerBenchmarks {
    private SqlServerConnection sqlServerConnection;
    private List<ProductOperation> productOperations;
    private List<ProductOperation> productOperationsToQuery;

    [Params(80000)] public int NumberOfValues { get; set; } = 80000;

    [GlobalSetup]
    public async Task Setup() {
        sqlServerConnection = new SqlServerConnection();
        await sqlServerConnection.SetUpDatabaseAndTables();
        productOperations = await ProductOperations();
        await sqlServerConnection.InsetProductOperationsIfNotExists(productOperations);
        await sqlServerConnection.InsetProductOperations2IfNotExists(productOperations);
        productOperationsToQuery = ProductOperationsToQuery(500);
    }
    
    //[Benchmark]
    public async Task BaseCase() {
        foreach (var productOperation in productOperationsToQuery) {
            var operations = await sqlServerConnection.QueryById(@$"
                SELECT TOP 1
                    OperationId,
                    OperationStatus,
                    ProductId,
                    OperationStartDate,
                    OperationEndDate,
                    OperationDetails
                FROM
                    dbo.{DB.ProductsOperationsTable}
                WHERE
                    ProductId = @ProductId
                ORDER BY
                    OperationStartDate DESC",
                "@ProductId", productOperation.ProductId);
            if (!operations.Single().Equals(productOperation)) throw new Exception("Read has fail!");
        }
    }

    //[Benchmark]
    public async Task WithComposedIndex() {
        foreach (var productOperation in productOperationsToQuery) {
            var operations = await sqlServerConnection.QueryById(@$"
                SELECT TOP 1
                    OperationId,
                    OperationStatus,
                    ProductId,
                    OperationStartDate,
                    OperationEndDate,
                    OperationDetails
                FROM
                    dbo.{DB.ProductsOperationsTable2}
                WHERE
                    ProductId = @ProductId
                ORDER BY
                    ProductId, OperationStartDate DESC",
                "@ProductId", productOperation.ProductId);
            if (!operations.Single().Equals(productOperation)) throw new Exception("Read has fail!");
        }
    }

    [Benchmark]
    public async Task AllInOneSingleQuery() {
        var operations = await sqlServerConnection.QueryByAllIds(@$"
            SELECT
                OperationId,
                OperationStatus,
                ProductId,
                OperationStartDate,
                OperationEndDate,
                OperationDetails
            FROM
                dbo.{DB.ProductsOperationsTable2}
            WHERE
                ProductId IN ( @ProductIds )
            ORDER BY
                ProductId, OperationStartDate DESC",
            "@ProductIds", productOperationsToQuery.Select(x => x.ProductId).ToList());
        var operationsGroupedByOperator = operations.GroupBy(x => x.ProductId);
        var lastOperations = new List<ProductOperation>();
        foreach (var group in operationsGroupedByOperator) {
            var lastOperation = group.ToList().First();
            lastOperations.Add(lastOperation);
        }
        if (!productOperationsToQuery.All(operation => lastOperations.Contains(operation))) throw new Exception("Read has fail!");
    }

    private async Task<List<ProductOperation>> ProductOperations() {
        const string productOperationsCsv = "./mssql-product-operations.csv";
        string basePath = Path.GetDirectoryName(WhereAmI());
        string targetPath = Path.Combine(basePath, productOperationsCsv);
        if (File.Exists(targetPath)) {
            var lines = await File.ReadAllLinesAsync(targetPath);
            return lines.Select(CreateProductOperationFromLine).ToList();
        }
        var OperationIds = Enumerable.Range(0, NumberOfValues / 100).Select(x => Guid.NewGuid().ToString()).ToList();

        var operations = Enumerable.Range(0, NumberOfValues)
            .Select(x => ProductOperation(OperationIds))
            .ToList();
        await File.WriteAllLinesAsync(targetPath, operations.Select(x => $"{x.Id},{(int)x.Status},{x.ProductId},{x.StartDate.Ticks},{x.EndDate.Ticks},{x.Details}"));
        return operations;
    }

    private List<ProductOperation> ProductOperationsToQuery(int numberOfQueries) {
        return Enumerable.Range(0, numberOfQueries).Select(x => {
            var randomIndex = new Random()
                .Next(0, NumberOfValues);
            var productId = productOperations[randomIndex].ProductId;
            var operationsByProductId = productOperations.Where(x => x.ProductId.Equals(productId)).ToList();
            var maxDateTime = operationsByProductId.Max(x => x.StartDate);
            return operationsByProductId.First(x => x.StartDate.Equals(maxDateTime));
        }).ToList();
    }

    private ProductOperation ProductOperation(List<string> operationIds) {
        var operationIdIndex = new Random().Next(0, operationIds.Count);
        return new ProductOperation(
            Id: Guid.NewGuid().ToString(),
            Status: (OperationStatus) new Random().Next(0, 4),
            ProductId: operationIds[operationIdIndex],
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
            new DateTime(long.Parse(args[3])),
            new DateTime(long.Parse(args[4])),
            args[5]);
    }
}
