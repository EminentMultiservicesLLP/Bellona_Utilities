using System;
using System.Collections.Generic;
using System.Drawing;
using System.Configuration;
using System.Threading.Tasks;
using System.Windows.Forms;

using log4net;
using log4net.Config;
using WIA;
using System.IO;
using System.Net.Http;
using iText.Kernel.Pdf;
using iText.Layout;
using iText.Kernel.Geom;
using iText.IO.Image;
using iText.Layout.Element;

/*
using iTextSharp.text.pdf; 

*/
namespace ScannerService
{
    public partial class Scanner : Form
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(Program));
        private const string ApiBaseUrl = "";
        private readonly string _scanFileFormatID = ConfigurationManager.AppSettings["scanFileFormat"].ToString();
        private readonly string _scanFileOutputLoc = ConfigurationManager.AppSettings["scanFileOutput"].ToString();
        private readonly string _scanHorizontalResolution = ConfigurationManager.AppSettings["scanHorizontalResolution"].ToString();
        private readonly string _scanVerticalResolution = ConfigurationManager.AppSettings["scanVerticalResolution"].ToString();
        private readonly string _scanBrightness = ConfigurationManager.AppSettings["scanBrightness"].ToString();
        private readonly string _scanContrast = ConfigurationManager.AppSettings["scanContrast"].ToString();
        private readonly string _scanColorMode = ConfigurationManager.AppSettings["scanColorMode"].ToString();
        private readonly string _scanAPIUrl = ConfigurationManager.AppSettings["scanAPIUrl"].ToString();
        private readonly string _scanAPIPostMethod = ConfigurationManager.AppSettings["scanAPIPostMethod"].ToString();
        private readonly string _enableSendBtn = ConfigurationManager.AppSettings["enableSendBtn"].ToString();
        private readonly string _sendFilepath = ConfigurationManager.AppSettings["sendFilepath"].ToString();

        
            
        int _progressPerc = 0;

        public Scanner()
        {
            InitializeComponent();
        }

        private void Scanner_Load(object sender, EventArgs e)
        {
            progressBar1.Maximum = 100;
            progressBar1.Minimum = 0;

            if(bool.Parse(_enableSendBtn))
            {
                btnSendDocument.Enabled = bool.Parse(_enableSendBtn);
            }
        }

        private void btnScan_Click(object sender, EventArgs e)
        {
            // Set the maximum value of the progress bar
            EnableDisableAll();
            _progressPerc = 1;
            progressTextBox.ResetText();
            backgroundPageScan.RunWorkerAsync();
        }

        private void EnableDisableScanButton(object sender)
        {
            if (btnScan.InvokeRequired)
            {
                btnScan.Invoke(new Action<object>(EnableDisableScanButton), btnScan);
            }
            else
            {
                btnScan.Enabled = !btnScan.Enabled;
            }
        }
        private void EnableDisableSendButton(object sender)
        {
            if (btnSendDocument.InvokeRequired)
            {
                btnSendDocument.Invoke(new Action<object>(EnableDisableSendButton), btnSendDocument);
            }
            else
            {
                btnSendDocument.Enabled = !btnSendDocument.Enabled;
            }
        }


        private void backgroundPageScan_DoWork(object sender, System.ComponentModel.DoWorkEventArgs e)
        {
            ScanPages();
        }

        private void ShowProgress(int perc)
        {
            if (progressBar1.InvokeRequired)
            {
                progressBar1.Invoke(new Action<int>(ShowProgress), perc);
            }
            else
            {
                // Update the progress bar value
                progressBar1.Value = perc;
            }

            //backgroundPageScan.ReportProgress(perc);
        }

        private IItem ConnectVia_DeviceManager()
        {
            DeviceInfo scanner = null;
            IItem item = null;
            try
            {
                // Connect to scanner
                var deviceManager = new DeviceManager();
                foreach (DeviceInfo deviceInfo in deviceManager.DeviceInfos)
                {
                    if (deviceInfo.Type == WiaDeviceType.ScannerDeviceType)
                    {
                        scanner = deviceInfo;
                        logInformation(LogType.Info, "  Scanner device found " + scanner.Properties["Name"].get_Value().ToString());
                        ShowProgress(_progressPerc += 10);
                        break;
                    }
                }

                // Scan document
                logInformation(LogType.Info, "  Connecting Scanner...");
                ShowProgress(_progressPerc += 5);
                var device = scanner.Connect();
                logInformation(LogType.Info, "  Scanner connected...");
                ShowProgress(_progressPerc += 5);
                item = device.Items[1];
            }
            catch (Exception ex)
            {
                logInformation(LogType.Error, "", ex);
            }

            if (scanner == null)
            {
                logInformation(LogType.Info, "Scanner not found.");
                return null;
            }
            return item;

        }

        private IItem ConnectVia_CommonDialog()
        {
            Device scannerDevice = null;
            IItem item = null;
            try
            {
                // Connect to scanner
                WIA.CommonDialog dialog = new WIA.CommonDialog();
                scannerDevice = dialog.ShowSelectDevice(WiaDeviceType.ScannerDeviceType, true, true);

                // Scan document
                logInformation(LogType.Info, "  Connecting Scanner...");
                ShowProgress(_progressPerc += 5);
                logInformation(LogType.Info, "  Scanner connected...");
                ShowProgress(_progressPerc += 5);
                item = scannerDevice.Items[1];
            }
            catch (Exception ex)
            {
                logInformation(LogType.Info, "", ex);
            }
            if (scannerDevice == null)
            {
                logInformation(LogType.Info, "Scanner not found.");
                return null;
            }
            return item;

        }
        
        public bool ScanPages()
        {
            bool isSuccess = false;
            try
            {
                ShowProgress(_progressPerc);
                logInformation(LogType.Info, "Connecting to Scanner...");

                IItem item = ConnectVia_DeviceManager();
                if (item == null)
                {
                    item = ConnectVia_CommonDialog();
                    if (item == null)
                    {
                        logInformation(LogType.Info, "No Scanner found");
                        return false;
                    }
                }


                //var ss = "";
                //foreach (Property item_1 in device.Items[1].Properties)
                //{
                //    ss += item_1.Name + " (" + item_1.PropertyID + ") " + item_1.get_Value() + Environment.NewLine;
                //}
                logInformation(LogType.Info, "      Configuraing Scanner before page scan...");
                ConfigureScanner(item); // Set scanner properties for better resolution
                logInformation(LogType.Info, "      Configured Scanner for page scan...");
                ShowProgress(_progressPerc += 10);

                logInformation(LogType.Info, "      Starting page scan...");
                var scannedPages = ScanMultiplePages(item);
                logInformation(LogType.Info, $"     Number of pages scanned: {scannedPages.Count}");
                ShowProgress(_progressPerc += 10);
                CombineScannedDocuments(scannedPages);

                /*
                // Process scanned images
                List<string> texts = new List<string>();
                logInformation(LogType.Info, "Processing scanned images...");
                foreach (var scannedPage in scannedPages)
                {
                    var bitmap = System.Drawing.Image.FromStream((Stream)scannedPage.FileData) as Bitmap;
                    //string text = ProcessImage(bitmap);
                    //texts.Add(text);
                }

                // Output the text
                logInformation(LogType.Info, "Scanned text:");
                foreach (var text in texts)
                {
                    log.Info(text);
                }
                */
                logInformation(LogType.Info, "Application finished.");
                isSuccess = true;
            }
            catch (Exception ex)
            {
                isSuccess = false;

                logInformation(LogType.Error, $"An error occurred: {ex.Message}");
            }
            finally
            {
                EnableDisableAll();
                ShowProgress(100);
            }
            return isSuccess;
        }

        private void ConfigureScanner(IItem item)
        {
            try
            {
                const string WIA_SCAN_COLOR_MODE = "6146";
                const string WIA_Format = "4106";// { B96B3CAB - 0728 - 11D3 - 9D7B - 0000F81EF32E}
                const string WIA_Brightness = "6154";
                const string WIA_Contrast = "6155";
                const string WIA_HorizontalResolution = "6147"; // 200
                const string WIA_VerticalResolution = "6148";// 200


                Property prop = item.Properties.get_Item(WIA_SCAN_COLOR_MODE);
                prop.set_Value(_scanColorMode);

                prop = item.Properties.get_Item(WIA_Format);
                prop.set_Value(_scanFileFormatID);

                prop = item.Properties.get_Item(WIA_Brightness);
                prop.set_Value(_scanBrightness);

                prop = item.Properties.get_Item(WIA_Contrast);
                prop.set_Value(_scanContrast);

                prop = item.Properties.get_Item(WIA_HorizontalResolution);
                prop.set_Value(_scanHorizontalResolution);

                prop = item.Properties.get_Item(WIA_VerticalResolution);
                prop.set_Value(_scanVerticalResolution);
            }
            catch (Exception ex)
            {
                logInformation(LogType.Error, $"Error configuring scanner: {ex.Message}");
            }
        }

        private List<ImageFile> ScanMultiplePages(IItem item)
        {
            List<ImageFile> scannedPages = new List<ImageFile>();
            try
            {
                int pageNum = 1;
                while (true)
                {
                    logInformation(LogType.Info, $"         Scanning page {pageNum}...");
                    ShowProgress(_progressPerc += 10);
                    var imageFile = (ImageFile)item.Transfer(_scanFileFormatID);
                    logInformation(LogType.Info, $"         Page {pageNum} scanned.");
                    ShowProgress(_progressPerc += 15);
                    scannedPages.Add(imageFile);

                    // Check if there are more pages to scan
                    var property = item.Properties["Document Handling Select"];
                    if (property != null && (Convert.ToInt32(property.get_Value()) & 0x01) == 0)
                    {
                        break; // No more pages
                    }

                    pageNum++;
                }
            }
            catch (Exception ex)
            {
                logInformation(LogType.Info, "          Error while scanning page", ex);
            }

            return scannedPages;
        }

        private System.Drawing.Image ConvertWIAImageFileToImage(ImageFile imageFile)
        {
            if (imageFile == null)
                return null;

            // Get image data as byte array
            byte[] imageData = (byte[])imageFile.FileData.get_BinaryData();

            // Create memory stream from byte array
            using (MemoryStream ms = new MemoryStream(imageData))
            {
                // Create Image from memory stream
                return System.Drawing.Image.FromStream(ms);
            }
        }

        private void CombineScannedDocuments(List<ImageFile> scannedImagePaths)
        {
            try
            {
                logInformation(LogType.Info, $"     Started Combining all scanned document into single file");
                ShowProgress(_progressPerc += 5);
                var pdfFilePath = $"{_scanFileOutputLoc}Scan_{DateTime.Now.ToString("ddMMyyyyHHmmss")}.pdf";

                // Get the first image as Bitmap to get pagesize
                Bitmap bitmap = (Bitmap)System.Drawing.Image.FromStream(new MemoryStream((byte[])scannedImagePaths[0].FileData.get_BinaryData()));
                using (PdfWriter writer = new PdfWriter(pdfFilePath))
                {
                    ShowProgress(_progressPerc += 5);
                    using (PdfDocument pdf = new PdfDocument(writer))
                    {
                        PageSize pageSize = new PageSize(bitmap.Width, bitmap.Height);
                        pdf.SetDefaultPageSize(pageSize);

                        ShowProgress(_progressPerc += 5);
                        Document document = new Document(pdf);
                        document.SetMargins(0, 0, 0, 0);

                        foreach (ImageFile image in scannedImagePaths)
                        {
                            // Convert WIA ImageFile to System.Drawing.Image
                            System.Drawing.Image img = System.Drawing.Image.FromStream(new MemoryStream(image.FileData.get_BinaryData()));
                            // Convert System.Drawing.Image to iText Image
                            iText.Layout.Element.Image pdfImage = new iText.Layout.Element.Image(iText.IO.Image.ImageDataFactory.Create(img, null));

                            // Add the image to the PDF document
                            document.Add(pdfImage);
                            ShowProgress(_progressPerc += 5);
                            /*

                            var ss = $"{_scanFileOutputLoc}Scan_{DateTime.Now.ToString("ddMMyyyyHHmmss")}.png";
                            image.SaveFile(ss);
                            string text = ReadTextFromImageAsync(ss);

                            // Add text to the PDF document
                            document.Add(new Paragraph(text));
                            */
                        }

                    }
                }

                if (File.Exists(pdfFilePath))
                    _ = UploadScannedImage(pdfFilePath);

                ShowProgress(_progressPerc += 10);
                logInformation(LogType.Info, $"     Completed Combining all scanned document into single file at {pdfFilePath}");
            }
            catch (Exception ex)
            {
                logInformation(LogType.Info, "      Error while combining files into single pdf", ex);
            }
        }

        private async Task UploadScannedImage(string filePath)
        {
            try
            {
                string apiUrl = $"{_scanAPIUrl}{_scanAPIPostMethod}"; // Change this to your API endpoint

                using (var httpClient = new HttpClient())
                using (var form = new MultipartFormDataContent())
                using (var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
                { 
                    // Add PDF file to the request
                    form.Add(new StreamContent(fileStream), "pdffile", System.IO.Path.GetFileName(filePath));

                    // Make POST request to the API
                    var response = await httpClient.PostAsync(apiUrl, form);

                    if (response.IsSuccessStatusCode)
                    {
                        // Read response content
                        string apiResponse = await response.Content.ReadAsStringAsync();
                        MessageBox.Show("API Response: " + apiResponse, "Success", MessageBoxButtons.OK, MessageBoxIcon.Information);
                    }
                    else
                    {
                        MessageBox.Show("API request failed: " + response.ReasonPhrase, "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
                    }
                }

            }
            catch (Exception ex)
            {
                logInformation(LogType.Error, $"An error occurred while uploading the image: {ex.Message}");
            }
            /*
            try
            {

                // Convert image to byte array
                byte[] imageBytes;
                using (var stream = new MemoryStream())
                {
                    bitmap.Save(stream, System.Drawing.Imaging.ImageFormat.Jpeg);
                    imageBytes = stream.ToArray();
                }

                // Create multipart form data content
                var formData = new MultipartFormDataContent();
                formData.Add(new ByteArrayContent(imageBytes), "scannedImage", "scannedImage.jpg");

                // Post the image to the API
                var response = await httpClient.PostAsync(ApiBaseUrl, formData);
                response.EnsureSuccessStatusCode();
            }
            catch (Exception ex)
            {
                logInformation(LogType.Error, $"An error occurred while uploading the image: {ex.Message}");
            }
            */
        }

        private void logInformation(LogType _logType, string message, Exception ex = null)
        {
            
            var logMessage = ex == null ? message : $"{message} \n {ex.Message} \n {ex.StackTrace}";
            switch (_logType)
            {
                case LogType.Info:
                    log.Info(logMessage);
                    break;
                case LogType.Error:
                    log.Error(logMessage);
                    break;
                case LogType.Warning:
                    log.Warn(logMessage);
                    break;
                default:
                    break;
            }
            if (progressTextBox.InvokeRequired)
            {
                progressTextBox.Invoke(new Action<LogType, string, Exception>(logInformation), _logType, message, null);
            }
            else
            {
                // Update the progress bar value
                progressTextBox.AppendText($"\n{message}");
                progressTextBox.ScrollToCaret();
            }
            
        }

        private void button1_Click(object sender, EventArgs e)
        {
            EnableDisableAll();
            progressTextBox.ResetText();
            var _sendfile = _sendFilepath;
            if(string.IsNullOrWhiteSpace(_sendFilepath))
            {
                openFileDialog1.DefaultExt = "pdf";
                openFileDialog1.Filter = "pdf files (*.pdf)|*.pdf";

                if (openFileDialog1.ShowDialog() == DialogResult.OK)
                {
                    _sendfile = openFileDialog1.FileName;
                }
            }
            
            _ = UploadScannedImage(_sendfile);
            EnableDisableAll();
        }

        private void EnableDisableAll()
        {
            EnableDisableSendButton(null);
            EnableDisableScanButton(null);
        }

    }
}
