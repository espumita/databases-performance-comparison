namespace AzureCosmosDB.Model;

public record Item(string id);

public record SampleItem(string id, string TenantId, string UserId, string SessionId, string Data): Item(id);

public record Product(string id, IEnumerable<Tenant> tenants);

public record Tenant(string TenantId, IEnumerable<User> users);

public record User(string UserId, IEnumerable<Session> sessions);

public record Session(string SessionId, string Data);

public record ProductOption2(string id, IEnumerable<TenantUserAndSession> Rows);

public record TenantUserAndSession(string TenantUserAndSessionId, string Data);

public record Option2SampleItem(string id, string TenantUserAndSessionId, string Data) : Item(id);

public record Option3SampleItem(string id, string Data) : Item(id);
