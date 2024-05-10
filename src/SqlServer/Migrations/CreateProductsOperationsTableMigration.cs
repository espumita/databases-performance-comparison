using FluentMigrator;

namespace SqlServer.Migrations;


[Migration(20240508173100)]
public class CreateProductsOperationsTableMigration : Migration {
    public override void Up() {
        Execute.Sql(@$"
            CREATE TABLE 
                dbo.[{DB.ProductsOperationsTable}]
            (
                OperationId VARCHAR(255) NOT NULL,
                OperationStatus INT NOT NULL,
                ProductId VARCHAR(255) NOT NULL,
                OperationStartDate DATETIME,
                OperationEndDate DATETIME,
                OperationDetails VARCHAR(255) NOT NULL,
                CONSTRAINT PK_{DB.ProductsOperationsTable} PRIMARY KEY (OperationId)
            )
        ");
        Execute.Sql(@$"
            CREATE TABLE 
                dbo.[{DB.ProductsOperationsTable2}]
            (
                OperationId VARCHAR(255) NOT NULL,
                OperationStatus INT NOT NULL,
                ProductId VARCHAR(255) NOT NULL,
                OperationStartDate DATETIME,
                OperationEndDate DATETIME,
                OperationDetails VARCHAR(255) NOT NULL,
                CONSTRAINT PK_{DB.ProductsOperationsTable2} PRIMARY KEY (OperationId)
            )
        ");
        Execute.Sql($@"
            CREATE NONCLUSTERED INDEX
                nci_{DB.ProductsOperationsTable2}_ProductId_OperationStartDate
            ON dbo.[{DB.ProductsOperationsTable2}]
                (ProductId, OperationStartDate DESC)
        ");
    }

    public override void Down() {
        Execute.Sql($"DROP TABLE dbo.[{DB.ProductsOperationsTable}]");
        Execute.Sql($"DROP TABLE dbo.[{DB.ProductsOperationsTable2}]");
    }
}