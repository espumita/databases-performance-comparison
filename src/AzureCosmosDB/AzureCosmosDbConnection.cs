using AzureCosmosDB.Model;
using Microsoft.Azure.Cosmos;
using User = AzureCosmosDB.Model.User;

namespace AzureCosmosDB;

public class AzureCosmosDbConnection {
    private const string DatabaseId = "DatabaseForBenchmarks";
    public const string Container1Id = "Container1";
    public const string Container2Id = "Container2";
    public const string Container3Id = "Container3";
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

    public async Task SetUpDatabaseAndContainers() {
        Database database = await client.CreateDatabaseIfNotExistsAsync(
            id: DatabaseId,
            throughput: RUsForTheDatabase
        );
        await database.CreateContainerIfNotExistsAsync(
            id: Container1Id,
            partitionKeyPath: PartitionKeyPath
        );
        await database.CreateContainerIfNotExistsAsync(
            id: Container2Id,
            partitionKeyPath: PartitionKeyPath
        );
        await database.CreateContainerIfNotExistsAsync(
            id: Container3Id,
            partitionKeyPath: PartitionKeyPath
        );
        await database.CreateContainerIfNotExistsAsync(new ContainerProperties {
                Id = Container4Id,
                PartitionKeyPaths = new List<string> {
                    "/TenantId",
                    "/UserId",
                    "/SessionId"
                }
            }
        );
    }

    public async Task InsetOption1ItemsIfNotExists(List<SampleItem> items) {
        var containerId = Container1Id;
        var oneItem = await Query<SampleItem>(@$"
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
                c.id = '{items[0].id}'
                AND t.TenantId = '{items[0].TenantId}'
                AND u.UserId = '{items[0].UserId}'
                AND s.SessionId = '{items[0].SessionId}'
        ",
            containerId,
            new QueryRequestOptions {
                PartitionKey = new PartitionKey(items[0].id)
            });
        if (oneItem.Count > 0) return;
        var groupsByIds = items.GroupBy(x => x.id);

        var products = new List<Product>();
        foreach (var groupsById in groupsByIds) {
            var id = groupsById.Key;
            var tenants = new List<Tenant>();
            var product = new Product(id, tenants);
            products.Add(product);
            var groupsByTenantIds = groupsById.GroupBy(x => x.TenantId);
            foreach (var groupsByTenantId in groupsByTenantIds) {
                var tenantId = groupsByTenantId.Key;
                var users = new List<User>();
                tenants.Add(new Tenant(tenantId, users));
                var groupsByUsersIds = groupsByTenantId.GroupBy(x => x.UserId);
                foreach (var groupsByUsersId in groupsByUsersIds) {
                    var userId = groupsByUsersId.Key;
                    var session = new List<Session>();
                    users.Add(new User(userId, session));
                    var groupsBySessionIds = groupsByUsersId.GroupBy(x => x.SessionId);
                    foreach (var groupsBySessionsIds in groupsBySessionIds) {
                        var sessionId = groupsBySessionsIds.Key;
                        var sampleItem = groupsBySessionsIds.Single(x => x.SessionId.Equals(sessionId));
                        session.Add(new Session(sessionId, sampleItem.Data));
                    }
                }
            }
        }

        foreach (var product in products) {
            var container = client.GetContainer(DatabaseId, containerId);
            await container.UpsertItemAsync(
                product,
                new PartitionKey(product.id)
            );
        }
    }

    public async Task InsetOption2ItemsIfNotExists(List<Option2SampleItem> items) {
        var containerId = Container2Id;
        var oneItem = await Query<Option2SampleItem>(@$"
            SELECT
                c.id,
                t.TenantUserAndSessionId,
                t.Data
            FROM 
                c
            JOIN
                 t IN c.Rows
            WHERE
                c.id = '{items[0].id}'
                AND t.TenantUserAndSessionId = '{items[0].TenantUserAndSessionId}'
        ",
            containerId,
            new QueryRequestOptions {
                PartitionKey = new PartitionKey(items[0].id)
            });
        if (oneItem.Count > 0) return;
        var groupsByIds = items.GroupBy(x => x.id);

        var products = groupsByIds.Select(groupsById => new ProductOption2(
            groupsById.Key,
            groupsById.Select(group => new TenantUserAndSession(
                group.TenantUserAndSessionId,
                group.Data
            )).ToList()
        )).ToList();

        foreach (var product in products) {
            var container = client.GetContainer(DatabaseId, containerId);
            await container.UpsertItemAsync(
                product,
                new PartitionKey(product.id)
            );
        }
    }

    public async Task InsetOption3ItemsIfNotExists(List<Option3SampleItem> option3SamplesItems) {
        var containerId = Container3Id;
        var oneItem = await ReadItemAsync<Option3SampleItem>(
            option3SamplesItems[0].id,
            AzureCosmosDbConnection.Container3Id,
            new PartitionKey(option3SamplesItems[0].id)
        );
        if (oneItem != null) return;
        foreach (var option4SampleItem in option3SamplesItems) {
            var container = client.GetContainer(DatabaseId, containerId);
            await container.UpsertItemAsync(
                option4SampleItem,
                new PartitionKey(option4SampleItem.id)
            );
        }
    }

    public async Task InsetOption4ItemsIfNotExists(List<SampleItem> samplesItems) {
        var containerId = Container4Id;
        var oneItem = await ReadItemAsync<SampleItem>(
            samplesItems[0].id,
            containerId,
            new PartitionKeyBuilder()
                .Add(samplesItems[0].TenantId)
                .Add(samplesItems[0].UserId)
                .Add(samplesItems[0].SessionId)
                .Build());
        if (oneItem != null) return;
        foreach (var option4SampleItem in samplesItems) {
            var container = client.GetContainer(DatabaseId, containerId);
            await container.UpsertItemAsync(
                option4SampleItem,
                new PartitionKeyBuilder()
                    .Add(option4SampleItem.TenantId)
                    .Add(option4SampleItem.UserId)
                    .Add(option4SampleItem.SessionId)
                    .Build()
            );
        }
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

    public async Task<T?> ReadItemAsync<T>(string id, string containerId, PartitionKey partitionKey) {
        var container = client.GetContainer(DatabaseId, containerId);
        T? readItemAsync = default(T);
        try {
            readItemAsync = await container.ReadItemAsync<T>(
                id,
                partitionKey
            );
        } catch (CosmosException exception) {
            if (exception.Message.Contains("404")) return default(T);
            throw;
        }
        return readItemAsync;
    }
}

