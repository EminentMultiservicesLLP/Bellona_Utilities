using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;  // Make sure to add System.Configuration if you use App.config for connection strings

namespace ScannerAPI.Common
{

    public class DatabaseHelper : IDisposable
    {
        private readonly string _connectionString = ConfigurationManager.AppSettings["ConnectionString"].ToString();
        private SqlConnection _connection;
        private SqlTransaction _transaction;

        public DatabaseHelper()
        {
            _connection = new SqlConnection(_connectionString);
        }


        public void OpenConnection()
        {
            if (_connection.State != ConnectionState.Open)
            {
                _connection.Open();
            }
        }
        public void BeginTransaction()
        {
            if (_connection.State == ConnectionState.Open)
            {
                _transaction = _connection.BeginTransaction();
            }
            else
            {
                throw new InvalidOperationException("Connection must be open to begin a transaction.");
            }
        }

        public void CommitTransaction()
        {
            _transaction?.Commit();
        }

        public void RollbackTransaction()
        {
            _transaction?.Rollback();
        }

        // Method to execute a non-query (INSERT, UPDATE, DELETE)
        public int ExecuteNonQuery(string query, CommandType commandType, List<SqlParameter> parameters = null)
        {
            //using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                using (SqlCommand command = new SqlCommand(query, _connection, _transaction))
                {
                    command.CommandType = commandType;
                    if (parameters != null)
                    {
                        command.Parameters.AddRange(parameters.ToArray());
                    }

                    OpenConnection();
                    return command.ExecuteNonQuery();
                }
            }
        }

        // Method to execute a scalar query (return a single value)
        public object ExecuteScalar(string query, CommandType commandType, List<SqlParameter> parameters = null)
        {
            //using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                using (SqlCommand command = new SqlCommand(query, _connection, _transaction))
                {
                    command.CommandType = commandType;
                    if (parameters != null)
                    {
                        command.Parameters.AddRange(parameters.ToArray());
                    }

                    OpenConnection();
                    return command.ExecuteScalar();
                }
            }
        }

        // Method to execute a query and return a DataTable
        public DataTable ExecuteQuery(string query, CommandType commandType, List<SqlParameter> parameters = null)
        {
            //using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                using (SqlCommand command = new SqlCommand(query, _connection, _transaction))
                {
                    command.CommandType = commandType;
                    if (parameters != null)
                    {
                        command.Parameters.AddRange(parameters.ToArray());
                    }

                    using (SqlDataAdapter adapter = new SqlDataAdapter(command))
                    {
                        DataTable dataTable = new DataTable();
                        adapter.Fill(dataTable);
                        return dataTable;
                    }
                }
            }
        }

        // Method to execute a stored procedure and return a DataTable
        public DataTable ExecuteStoredProcedure(string storedProcedureName, List<SqlParameter> parameters = null)
        {
            return ExecuteQuery(storedProcedureName, CommandType.StoredProcedure, parameters);
        }

        // Helper method to create SQL parameters
        public SqlParameter CreateParameter(string name, object value, SqlDbType dbType, ParameterDirection direction = ParameterDirection.Input)
        {
            SqlParameter parameter = new SqlParameter
            {
                ParameterName = name,
                Value = value ?? DBNull.Value,
                SqlDbType = dbType,
                Direction = direction
            };

            return parameter;
        }


        public void Dispose()
        {
            _transaction?.Dispose();
            if (_connection.State == ConnectionState.Open)
            {
                _connection.Close();
            }
            _connection.Dispose();
        }
    }

}