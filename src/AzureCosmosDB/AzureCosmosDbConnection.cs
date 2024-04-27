using Microsoft.Azure.Cosmos;

namespace AzureCosmosDB;

public class AzureCosmosDbConnection {
    private const string DatabaseId = "DatabaseForBenchmarks";
    private const string Container01Id = "Container01";
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
            id: Container01Id,
            partitionKeyPath: PartitionKeyPath
        );
    }

    public async Task InsetItem<T>(T item){
        var container = client.GetContainer(DatabaseId, Container01Id);
        await container.UpsertItemAsync(item
        //    , new PartitionKey((item as Item).id)
        );
    }

    public async Task<List<T>> Query<T>(string query, QueryRequestOptions options = null) {
        var container = client.GetContainer(DatabaseId, Container01Id);
        var queryIterator = container.GetItemQueryIterator<T>(query, null, options);
        var result = new List<T>();
        while (queryIterator.HasMoreResults) {
            var response = await queryIterator.ReadNextAsync();
            result.AddRange(response);
        }
        return result;
    }
            
    public async Task InsetItemsIfNotExists<T>(List<T> items) {
        var oneItem = await Query<T>(@$"
            SELECT 
                *
            FROM 
                c
            WHERE 
                c.id = '{(items[0] as Item).id}'
        "
        //    , new QueryRequestOptions {
        //    PartitionKey = new PartitionKey((items[0] as Item).id)
        //}
            );
        if (oneItem.Count > 0) return;
        foreach (var item in items) {
            await InsetItem(item);
        }
    }
}

public record Item(string id);

