namespace AzureCosmosDB.Model;

public record Item(string id);

public record SampleItem(string id, string TenantId, string UserId, string SessionId, string Data): Item(id);

public record Option4SampleItem(string id, string Data) : Item(id);