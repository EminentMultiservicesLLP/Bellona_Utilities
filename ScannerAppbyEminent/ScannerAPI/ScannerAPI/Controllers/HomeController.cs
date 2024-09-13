using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using System.Configuration;
using Newtonsoft.Json.Linq;
using System.Text.RegularExpressions;
using log4net;
using ScannerAPI.Common;

namespace ScannerAPI.Controllers
{
    //[Authorize] // Requires authentication (if using ASP.NET Identity Core)
    public class HomeController : ApiController
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(HomeController));
        string _tessdata = ConfigurationManager.AppSettings["tessdata"].ToString();
        string _imgReceivedStorePath = ConfigurationManager.AppSettings["imgReceivedStorePath"].ToString();
        string _imgOutputPath = ConfigurationManager.AppSettings["imgOutputPath"].ToString();
        string _dataExtractSetting = ConfigurationManager.AppSettings["dataExtractSetting"].ToString();
        string _writeOutputTo = ConfigurationManager.AppSettings["writeOutputTo"].ToString();
        string _writeOutputtoText = ConfigurationManager.AppSettings["writeOutputtoText"].ToString();

        [HttpPost]
        public async Task<IHttpActionResult> ProcessScannedDocument()
        {
            Logging.LogInformation(LogType.Info, "API call received for ProcessScannedDocument Method");
            if (!Request.Content.IsMimeMultipartContent())
            {
                Logging.LogInformation(LogType.Error, "API call for ProcessScannedDocument failed due to UnsupportedMediaType error");
                return StatusCode(HttpStatusCode.UnsupportedMediaType);
            }

            var provider = new MultipartMemoryStreamProvider();
            try
            {
                Logging.LogInformation(LogType.Info, "  Call to Request.Content.ReadAsMultipartAsync(provider)");
                await Request.Content.ReadAsMultipartAsync(provider);

                Logging.LogInformation(LogType.Info, "  Processing provider contents");
                foreach (var file in provider.Contents)
                {
                    var filename = file.Headers.ContentDisposition.FileName.Trim('\"');
                    Logging.LogInformation(LogType.Info, "  Read content as Stream");
                    var fileContent = await file.ReadAsStreamAsync();

                    // Save Content to Server
                    Logging.LogInformation(LogType.Info, "      Check if directory for storing received image from source exists, if not then create :" + _imgReceivedStorePath);
                    if (!Directory.Exists(_imgReceivedStorePath)) Directory.CreateDirectory(_imgReceivedStorePath);

                    string filePath_receivedFile = Path.Combine(_imgReceivedStorePath, Guid.NewGuid().ToString() + filename);
                    Logging.LogInformation(LogType.Info, "  Received file to be saved at location :" + filePath_receivedFile);
                    using (FileStream fileStream = File.Create(filePath_receivedFile))
                    {
                        await fileContent.CopyToAsync(fileStream);


                    }
                    Logging.LogInformation(LogType.Info, "  Received file saved at location :" + filePath_receivedFile);

                    if (System.IO.Path.GetExtension(filename).Equals(".pdf", StringComparison.InvariantCultureIgnoreCase))
                    {
                        Logging.LogInformation(LogType.Info, "  Starting Read PDF content");
                        var pdfText = ExtractText_PDF.SaveImages(filePath_receivedFile, filename, _imgOutputPath, _tessdata);
                        Logging.LogInformation(LogType.Info, "  Completed Read PDF content");

                        DatabaseActivities dbAct = new DatabaseActivities();
                        Logging.LogInformation(LogType.Info, "  Insert Document details into database");
                        int docId = dbAct.InsertDocumentDetail(filename, filePath_receivedFile, pdfText);
                        Logging.LogInformation(
                            docId > 0 ? LogType.Info : LogType.Error,
                            docId > 0 ? $"Successfully Inserted Document details into database with document id: {docId}" : "Something went wrong while inserting details into the database"
                        );

                        Logging.LogInformation(LogType.Info, "  Inserting properties values required in database");
                        Dictionary<string, string> dataDict = dbAct.SaveExtractedData(docId, pdfText);
                        Logging.LogInformation(LogType.Info, "  Inserted properties values in database");

                        string dictionaryString = "{";
                        foreach (KeyValuePair<string, string> keyValues in dataDict)
                        {
                            dictionaryString += keyValues.Key + " : " + keyValues.Value + ", ";
                        }
                        dictionaryString = dictionaryString.TrimEnd(',', ' ') + "}";
                        Logging.LogInformation(LogType.Info, "  Text extracted from file and prepared for sharing with source " + Environment.NewLine + dictionaryString);

                        if (Boolean.Parse(_writeOutputtoText))
                        {
                            var write_outputto = _imgReceivedStorePath + "\\Output.txt";
                            using (StreamWriter writer = new StreamWriter(write_outputto))
                            {
                                // Write the text to the file
                                writer.Write($"######   {DateTime.Now.ToString("ddMMyyyyHHmmss")}  FileReceived - {filename} stored at location {filePath_receivedFile}");
                                writer.WriteLine(dictionaryString);
                                writer.WriteLine("");
                            }
                        }

                        return Ok(dictionaryString);
                    }
                    else
                    {
                        Logging.LogInformation(LogType.Error, "  Failed due to Invalid file format. Please upload a PDF file.");
                        return BadRequest("Invalid file format. Please upload a PDF file.");
                    }
                }
                return BadRequest("No file found in request.");
            }
            catch (Exception ex)
            {
                Logging.LogInformation(LogType.Error, "  Failed API Call.", ex);
                return InternalServerError(ex);
            }
        }

        private Dictionary<string, string> ExtractRequiredText_NotInUse(string stext)
        {
            Dictionary<string, string> dataExtractList = new Dictionary<string, string>();
            try
            {
                // Read all text from the JSON file
                string jsonContent = File.ReadAllText(_dataExtractSetting);
                var jsonData = Newtonsoft.Json.JsonConvert.DeserializeObject<List<ExtractSettings>>(jsonContent);

                // Parse JSON content into JArray
                JArray jsonArray = JArray.Parse(jsonContent);
                foreach (var row in jsonArray)
                {
                    JObject jsonObject = (JObject)row;

                    var property_start = Regex.Replace(jsonObject["Start"].ToString(), @"\s+", "").Trim();
                    var property_End = Regex.Replace(jsonObject["End"].ToString(), @"\s+", "").Trim();
                    var property_LinkTo = Regex.Replace(jsonObject["LinkTo"].ToString(), @"\s+", "").Trim();

                    /*
                    foreach (var property in jsonObject.Properties())
                    {
                        var propertyName_start = Regex.Replace(property.Name.ToString(), @"\s+", "").Trim();
                        var propertyValue_start = Regex.Replace(property.Value.ToString(), @"\s+", "").Trim();

                        var propertyName_end = Regex.Replace(property.Name.ToString(), @"\s+", "").Trim();
                        var propertyValue_end = Regex.Replace(property.Value.ToString(), @"\s+", "").Trim();
                    }
                    */

                    var startIdex = stext.IndexOf(property_start) + property_start.Length;
                    var endIndex = stext.IndexOf(property_End) - startIdex;
                    dataExtractList.Add(row["LinkTo"].ToString(), stext.Substring(startIdex, endIndex));
                }
            }
            catch (Exception ex)
            {
                Logging.LogInformation(LogType.Error, "  Error reading JSON file.", ex);
                return null;
            }
            return dataExtractList;
        }

        

    
    }



    public class ExtractSettings
    {
        public string Start { get; set; }
        public string End { get; set; }
        public string LinkTo { get; set; }
    }

}
