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
    }

    public override void Down() {
        Execute.Sql($"DROP TABLE dbo.[{DB.ProductsOperationsTable}]");
    }
}