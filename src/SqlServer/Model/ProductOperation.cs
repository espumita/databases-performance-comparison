namespace SqlServer.Model;

public record ProductOperation(string Id, OperationStatus Status, string ProductId, DateTime StartDate, DateTime EndDate, string Details);

public enum OperationStatus {
    InProgress,
    Success,
    Failure,
    Cancelled
}