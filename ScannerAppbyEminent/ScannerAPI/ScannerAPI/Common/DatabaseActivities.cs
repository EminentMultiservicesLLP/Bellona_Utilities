using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.SqlClient;
using System.Data;
using System.Text.RegularExpressions;

namespace ScannerAPI.Common
{
    class DatabaseActivities
    {

        internal int InsertDocumentDetail(string fileName, string filePath, string fileContent)
        {
            int outputDocId = 0;
            using (DatabaseHelper dbHelper = new DatabaseHelper())
            {
                try
                {
                    Logging.LogInformation(LogType.Info, "      Opening SQL Connection.");
                    dbHelper.OpenConnection();
                    Logging.LogInformation(LogType.Info, "      Opening SQL Transactions.");
                    dbHelper.BeginTransaction();

                    Logging.LogInformation(LogType.Info, "      Creating SQLParameters for first SP [dbo].[UpsertDMS_Document] call.");
                    List<SqlParameter> sParams = new List<SqlParameter>();
                    sParams.Add(new SqlParameter("FileName", fileName));
                    sParams.Add(new SqlParameter("FilePath", filePath));
                    SqlParameter outputParam = new SqlParameter("DocumentId", System.Data.ParameterDirection.Output);
                    sParams.Add(outputParam);
                    Logging.LogInformation(LogType.Info, "      SQLParameters list created for first SP [dbo].[UpsertDMS_Document] call.");

                    dbHelper.ExecuteNonQuery("[dbo].[UpsertDMS_Document]", CommandType.StoredProcedure, sParams);
                    Logging.LogInformation(LogType.Info, "      SP [dbo].[UpsertDMS_Document]  successfully executed.");

                    outputDocId = (int)outputParam.Value;
                    Logging.LogInformation(LogType.Info, "      Output DocId from first SP: " + outputDocId);

                    Logging.LogInformation(LogType.Info, "      Creating SQLParameters for first SP [dbo].[UpsertDMS_ScanDocumentContent] call.");
                    sParams = new List<SqlParameter>();
                    sParams.Add(new SqlParameter("DocId", outputDocId));
                    sParams.Add(new SqlParameter("FileContent", fileContent));
                    Logging.LogInformation(LogType.Info, "      SQLParameters list created for second SP [dbo].[UpsertDMS_ScanDocumentContent] call.");

                    dbHelper.ExecuteNonQuery("[dbo].[UpsertDMS_ScanDocumentContent]", CommandType.StoredProcedure, sParams);
                    Logging.LogInformation(LogType.Info, "      SP [dbo].[UpsertDMS_ScanDocumentContent]  successfully executed.");
                    // Commit the transaction if all operations succeed
                    dbHelper.CommitTransaction();
                    Logging.LogInformation(LogType.Info, "      Transaction committed");

                    return outputDocId;
                }
                catch (Exception ex)
                {
                    // Rollback the transaction if any operation fails
                    dbHelper.RollbackTransaction();
                    Logging.LogInformation(LogType.Error, "     Transaction rolled back due to an error: " + ex.Message);
                    return outputDocId;
                }
            }
        }

        internal Dictionary<string, string> SaveExtractedData(int docId, string sPDFText)
        {
            Dictionary<string, string> dataExtractList = new Dictionary<string, string>();
            using (DatabaseHelper dbHelper = new DatabaseHelper())
            {
                try
                {
                    DataTable dtProperties = dbHelper.ExecuteStoredProcedure("dbo.dbsp_SelectAllProperties");
                    if (dtProperties != null && dtProperties.Rows.Count > 0)
                    {
                        // Create a DataTable with the same structure as the user-defined table type
                        var propertiesValuesTable = new DataTable();
                        propertiesValuesTable.Columns.Add("DocumentId", typeof(int));
                        propertiesValuesTable.Columns.Add("PropertyId", typeof(int));
                        propertiesValuesTable.Columns.Add("PropertyContent", typeof(string));
                        propertiesValuesTable.Columns.Add("CreatedOn", typeof(DateTime));
                        propertiesValuesTable.Columns.Add("UpdateOn", typeof(DateTime));
                        propertiesValuesTable.Columns.Add("Deactive", typeof(bool));

                        foreach (DataRow dr in dtProperties.Rows)
                        {
                            var property_start = Regex.Replace(dr["PropertyStart"].ToString(), @"\s+", "").Trim();
                            var property_End = Regex.Replace(dr["PropertyEnd"].ToString(), @"\s+", "").Trim();
                            var property_LinkTo = Regex.Replace(dr["PropertyName"].ToString(), @"\s+", "").Trim();
                            var startIdex = sPDFText.IndexOf(property_start) + property_start.Length;
                            var endIndex = sPDFText.IndexOf(property_End) - startIdex;

                            var extractedValue = sPDFText.Substring(startIdex, endIndex);
                            dataExtractList.Add(dr["PropertyName"].ToString(), extractedValue);
                            propertiesValuesTable.Rows.Add(docId, dr["PropertyId"], extractedValue, DateTime.Now, DateTime.Now, false);
                        }

                        // Add the table-valued parameter
                        var tvpParam = new SqlParameter
                        {
                            ParameterName = "@PropertiesValues",
                            SqlDbType = SqlDbType.Structured,
                            TypeName = "dbo.DMS_Properties_Value_Type",
                            Value = propertiesValuesTable
                        };

                        List<SqlParameter> sParams = new List<SqlParameter>();
                        sParams.Add(tvpParam);

                        dbHelper.ExecuteNonQuery("dbo.InsertDMSPropertiesValues", CommandType.StoredProcedure, sParams);
                    }
                }
                catch (Exception ex)
                {
                    // Rollback the transaction if any operation fails
                    dbHelper.RollbackTransaction();
                    Logging.LogInformation(LogType.Error, "     Transaction rolled back due to an error: " + ex.Message);
                }
                return dataExtractList;
            }
        }


    }
}
