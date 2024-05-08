using System.Reflection;
using FluentMigrator.Runner;
using FluentMigrator.Runner.Initialization;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using SqlServer.Migrations;

namespace SqlServer
{
    public class SqlServerConnection {
        private const string MssqlSaPassword = "ABCdef123|+.";

        public async Task SetUpDatabaseAndTables() {
            using (SqlConnection connection = new SqlConnection(ConnectionString("master", MssqlSaPassword))) {
                connection.Open();
                SqlCommand databaseExitsCommand = new SqlCommand($"SELECT COUNT(*) FROM sys.databases WHERE name = '{DB.DataBaseName}'", connection);
                var result = await databaseExitsCommand.ExecuteScalarAsync();
                int count = Convert.ToInt32(result);
                if (count <= 0) {
                    SqlCommand createDatabaseCommand = new SqlCommand($"CREATE DATABASE [{DB.DataBaseName}]", connection);
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

        string ConnectionString(string databaseName, string mssqlSaPassword) {
            return $"Server=host.docker.internal; Database={databaseName}; User Id=sa;Password={mssqlSaPassword};TrustServerCertificate=true;";
        }
    }
}
