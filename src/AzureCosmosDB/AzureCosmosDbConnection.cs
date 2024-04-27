﻿using AzureCosmosDB.Model;
using Microsoft.Azure.Cosmos;

namespace AzureCosmosDB;

public class AzureCosmosDbConnection {
    private const string DatabaseId = "DatabaseForBenchmarks";
    public const string Container1Id = "Container1";
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
            id: Container1Id,
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

    public async Task InsetOption1ItemsIfNotExists(List<SampleItem> items) {
        var containerId = Container1Id;
        var oneItem = await Query<Product>(@$"
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
            where
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
                        var sessionIds = groupsBySessionsIds.Key;
                        var sampleItem = groupsBySessionsIds.Single(x => x.SessionId.Equals(sessionIds));
                        session.Add(new Session(sessionIds, sampleItem.Data));
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

public record Product(string id, IEnumerable<Tenant> tenants);

public record Tenant(string TenantId, IEnumerable<User> users);

public record User(string UserId, IEnumerable<Session> sessions);

public record Session(string SessionId, string Data);
