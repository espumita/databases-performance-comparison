using AzureCosmosDB;

namespace databases_performance_comparison.Model;

public record SampleItem(string id, string TenantId, string UserId, string SessionId, string Data): Item(id);