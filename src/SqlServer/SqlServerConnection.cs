using System.Reflection;
using FluentMigrator.Runner;
using FluentMigrator.Runner.Initialization;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using SqlServer.Migrations;
using SqlServer.Model;

namespace SqlServer {
    public class SqlServerConnection {
        private const string MssqlSaPassword = "ABCdef123|+.";

        public async Task SetUpDatabaseAndTables() {
            using (SqlConnection connection = new SqlConnection(ConnectionString("master", MssqlSaPassword))) {
                connection.Open();
                SqlCommand databaseExitsCommand =
                    new SqlCommand($"SELECT COUNT(*) FROM sys.databases WHERE name = '{DB.DataBaseName}'", connection);
                var result = await databaseExitsCommand.ExecuteScalarAsync();
                int count = Convert.ToInt32(result);
                if (count <= 0) {
                    SqlCommand createDatabaseCommand =
                        new SqlCommand($"CREATE DATABASE [{DB.DataBaseName}]", connection);
                    await createDatabaseCommand.ExecuteNonQueryAsync();
                }
            }

            var services = new ServiceCollection()
                .AddFluentMigratorCore()
                .ConfigureRunner(rb => rb.AddSqlServer()
                    .WithGlobalConnectionString(ConnectionString(DB.DataBaseName, MssqlSaPassword))
                    .WithMigrationsIn(Assembly.GetAssembly(typeof(DB)))).AddLogging(lb => lb.AddFluentMigratorConsole())
                .Configure<RunnerOptions>(opt => { opt.Tags = ["Benchmarks"]; })
                .BuildServiceProvider(false);
            var scope = services.CreateScope();
            var runner = scope.ServiceProvider.GetRequiredService<IMigrationRunner>();
            runner.ListMigrations();
            runner.MigrateUp();
        }

        private string ConnectionString(string databaseName, string mssqlSaPassword) {
            return
                $"Server=host.docker.internal; Database={databaseName}; User Id=sa;Password={mssqlSaPassword};TrustServerCertificate=true;";
        }

        public async Task InsetProductOperationsIfNotExists(List<ProductOperation> productOperations) {
            using (SqlConnection connection = new SqlConnection(ConnectionString(DB.DataBaseName, MssqlSaPassword))) {
                connection.Open();
                try {
                    SqlCommand command = new SqlCommand(@$"
                        SELECT
                            OperationId
                        FROM
                            dbo.{DB.ProductsOperationsTable}
                        WHERE
                            OperationId = @OperationId", connection);
                    command.Parameters.AddWithValue("@OperationId", productOperations[0].Id);
                    var result = await command.ExecuteScalarAsync();
                    if (result == null) {
                        foreach (var productOperation in productOperations) {
                            SqlCommand insertProductOperationCommand = new SqlCommand(@$"
                            INSERT INTO dbo.{DB.ProductsOperationsTable} (
                                OperationId,
                                OperationStatus,
                                ProductId,
                                OperationStartDate,
                                OperationEndDate,
                                OperationDetails
                            ) VALUES (
                                @OperationId,
                                @OperationStatus,
                                @ProductId,
                                @OperationStartDate,
                                @OperationEndDate,
                                @OperationDetails
                            );", connection);
                            insertProductOperationCommand.Parameters.AddWithValue("@OperationId", productOperation.Id);
                            insertProductOperationCommand.Parameters.AddWithValue("@OperationStatus", (int)productOperation.Status);
                            insertProductOperationCommand.Parameters.AddWithValue("@ProductId", productOperation.ProductId);
                            insertProductOperationCommand.Parameters.AddWithValue("@OperationStartDate", productOperation.StartDate);
                            insertProductOperationCommand.Parameters.AddWithValue("@OperationEndDate", productOperation.EndDate);
                            insertProductOperationCommand.Parameters.AddWithValue("@OperationDetails", productOperation.Details);
                            await insertProductOperationCommand.ExecuteNonQueryAsync();
                        };
                    }
                } finally {
                    connection.Close();
                }


            }

        }

        public async Task InsetProductOperations2IfNotExists(List<ProductOperation> productOperations) {
            using (SqlConnection connection = new SqlConnection(ConnectionString(DB.DataBaseName, MssqlSaPassword))) {
                connection.Open();
                try {
                    SqlCommand command = new SqlCommand(@$"
                        SELECT
                            OperationId
                        FROM
                            dbo.{DB.ProductsOperationsTable2}
                        WHERE
                            OperationId = @OperationId", connection);
                    command.Parameters.AddWithValue("@OperationId", productOperations[0].Id);
                    var result = await command.ExecuteScalarAsync();
                    if (result == null) {
                        foreach (var productOperation in productOperations) {
                            SqlCommand insertProductOperationCommand = new SqlCommand(@$"
                            INSERT INTO dbo.{DB.ProductsOperationsTable2} (
                                OperationId,
                                OperationStatus,
                                ProductId,
                                OperationStartDate,
                                OperationEndDate,
                                OperationDetails
                            ) VALUES (
                                @OperationId,
                                @OperationStatus,
                                @ProductId,
                                @OperationStartDate,
                                @OperationEndDate,
                                @OperationDetails
                            );", connection);
                            insertProductOperationCommand.Parameters.AddWithValue("@OperationId", productOperation.Id);
                            insertProductOperationCommand.Parameters.AddWithValue("@OperationStatus", (int)productOperation.Status);
                            insertProductOperationCommand.Parameters.AddWithValue("@ProductId", productOperation.ProductId);
                            insertProductOperationCommand.Parameters.AddWithValue("@OperationStartDate", productOperation.StartDate);
                            insertProductOperationCommand.Parameters.AddWithValue("@OperationEndDate", productOperation.EndDate);
                            insertProductOperationCommand.Parameters.AddWithValue("@OperationDetails", productOperation.Details);
                            await insertProductOperationCommand.ExecuteNonQueryAsync();
                        };
                    }
                } finally {
                    connection.Close();
                }


            }

        }

        public async Task<IEnumerable<ProductOperation>> QueryById(string query, string parameterName, string parameterValue) {
            var retults = new List<ProductOperation>();
            using (SqlConnection connection = new SqlConnection(ConnectionString(DB.DataBaseName, MssqlSaPassword))) {
                connection.Open();
                SqlCommand command = new SqlCommand(query, connection);
                command.Parameters.AddWithValue(parameterName, parameterValue);
                var reader = await command.ExecuteReaderAsync();
                try {
                    while (reader.Read()) {
                        retults.Add(new ProductOperation(
                            (string) reader["OperationId"],
                            (OperationStatus) reader["OperationStatus"],
                            (string) reader["ProductId"],
                            (DateTime) reader["OperationStartDate"], 
                            (DateTime) reader["OperationEndDate"],
                            (string) reader["OperationDetails"]
                        ));
                    }
                } finally {
                    reader.Close();
                }
            }
            return retults;
        }

        public async Task<IEnumerable<ProductOperation>> QueryByAllIds(string query, string parameterName, List<string> parameterValue) {
            var retults = new List<ProductOperation>();
            using (SqlConnection connection = new SqlConnection(ConnectionString(DB.DataBaseName, MssqlSaPassword))) {
                connection.Open();
                var parameterValues = string.Join(",", parameterValue.Select(x => $"'{x}'"));
                var queryWithValues = query.Replace(parameterName, parameterValues);
                SqlCommand command = new SqlCommand(queryWithValues, connection);
                var reader = await command.ExecuteReaderAsync();
                try {
                    while (reader.Read()) {
                        retults.Add(new ProductOperation(
                            (string)reader["OperationId"],
                            (OperationStatus)reader["OperationStatus"],
                            (string)reader["ProductId"],
                            (DateTime)reader["OperationStartDate"],
                            (DateTime)reader["OperationEndDate"],
                            (string)reader["OperationDetails"]
                        ));
                    }
                } finally {
                    reader.Close();
                }
            }
            return retults;
        }
    }
}
