using AzureCosmosDB.Model;
using Microsoft.Azure.Cosmos;

namespace AzureCosmosDB;

public class AzureCosmosDbConnection {
    private const string DatabaseId = "DatabaseForBenchmarks";
    public const string Container3AId = "Container3A";
    public const string Container3BId = "Container3B";
    public const string Container4Id = "Container4";
    private const string PartitionKeyPath = "/id";
    private const int RUsForTheDatabase = 400;
    private readonly CosmosClient client;
    
    public AzureCosmosDbConnection() {
        client  = new CosmosClient(
            accountEndpoint: "https://localhost:8081/",
            authKeyOrResourceToken: "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
        );
    }

    public async Task SetDatabaseAndContainers() {
        Database database = await client.CreateDatabaseIfNotExistsAsync(
            id: DatabaseId,
            throughput: RUsForTheDatabase
        );
        await database.CreateContainerIfNotExistsAsync(
            id: Container3AId,
            partitionKeyPath: PartitionKeyPath
        );
        await database.CreateContainerIfNotExistsAsync(
            id: Container3BId,
            partitionKeyPath: PartitionKeyPath
        );
        await database.CreateContainerIfNotExistsAsync(
            id: Container4Id,
            partitionKeyPath: PartitionKeyPath
        );
    }

    public async Task<List<T>> Query<T>(string query, string containerId, QueryRequestOptions options = null) {
        var container = client.GetContainer(DatabaseId, containerId);
        var queryIterator = container.GetItemQueryIterator<T>(query, null, options);
        var result = new List<T>();
        while (queryIterator.HasMoreResults) {
            var response = await queryIterator.ReadNextAsync();
            result.AddRange(response);
        }
        return result;
    }

    public async Task InsetOption3AItemsIfNotExists(List<SampleItem> items) {
        var containerId = Container3AId;
        var oneItem = await Query<Item>(@$"
            SELECT 
                *
            FROM 
                c
            WHERE 
                c.id = '{items[0].id}'
                AND c.TenantId = '{items[0].TenantId}'
                AND c.UserId = '{items[0].UserId}'
                AND c.SessionId = '{items[0].SessionId}'
        ",
        containerId);
        if (oneItem.Count > 0) return;
        foreach (var item in items) {
            var container = client.GetContainer(DatabaseId, containerId);
            await container.UpsertItemAsync(item);
        }
    }

    public async Task InsetOption3BItemsIfNotExists(List<SampleItem> items) {
        var containerId = Container3BId;
        var oneItem = await Query<Item>(@$"
            SELECT 
                *
            FROM 
                c
            WHERE 
                c.id = '{items[0].id}'
                AND c.TenantId = '{items[0].TenantId}'
                AND c.UserId = '{items[0].UserId}'
                AND c.SessionId = '{items[0].SessionId}'
        ",
        containerId,
        new QueryRequestOptions {
            PartitionKey = new PartitionKey(items[0].id)
        });
        if (oneItem.Count > 0) return;
        foreach (var item in items) {
            var container = client.GetContainer(DatabaseId, containerId);
            await container.UpsertItemAsync(
                item,
                new PartitionKey((item as Item).id)
            );
        }
    }

    public async Task InsetOption4ItemsIfNotExists(List<Option4SampleItem> option4SamplesItems) {
        var containerId = Container4Id;
        var oneItem = await Query<Item>(@$"
            SELECT 
                *
            FROM 
                c
            WHERE 
                c.id = '{option4SamplesItems[0].id}'
            ",
            containerId,
            new QueryRequestOptions {
                PartitionKey = new PartitionKey(option4SamplesItems[0].id)
            });
        if (oneItem.Count > 0) return;
        foreach (var option4SampleItem in option4SamplesItems) {
            var container = client.GetContainer(DatabaseId, containerId);
            await container.UpsertItemAsync(
                option4SampleItem,
                new PartitionKey(option4SampleItem.id)
            );
        }
    }
}
