using System.Data.Entity;
using log4net;
using NAPS2.Scan;
using NAPS2.Images.Gdi;
using NAPS2.Images;
using NAPS2.Scan.Exceptions;
using NAPS2.Pdf;
using System.Drawing.Imaging;
using System.Collections.Generic;
using System.Configuration;
using System.Windows.Forms;
using System.Runtime.InteropServices;

namespace ScannerApp
{
    public partial class frmScanner : Form
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(frmScanner));
        private ScanningContext _scanningContext;
        private ScanController _controller;
        private List<ScanDevice> _devices;

        private const string ApiBaseUrl = "";
        //private readonly string _scanFileFormatID = ConfigurationManager.AppSettings["scanFileFormat"].ToString();
        //private readonly string _scanFileOutputLoc_image = ConfigurationManager.AppSettings["scanFileOutput_Image"].ToString();
        //private readonly string _scanFileOutputLoc_pdf = ConfigurationManager.AppSettings["scanFileOutput_pdf"].ToString();
        //private readonly string _scanResolution = ConfigurationManager.AppSettings["scanResolution"].ToString();
        //private readonly string _scanBrightness = ConfigurationManager.AppSettings["scanBrightness"].ToString();
        //private readonly string _scanContrast = ConfigurationManager.AppSettings["scanContrast"].ToString();
        //private readonly string _scanColorMode = ConfigurationManager.AppSettings["scanColorMode"].ToString();
        //private readonly string _scanAPIUrl = ConfigurationManager.AppSettings["scanAPIUrl"].ToString();
        ////private readonly string _scanAPIPostMethod = ConfigurationManager.AppSettings["scanAPIPostMethod"].ToString();

        private readonly string _scanFileOutputLoc_image = ConfigurationManager.AppSettings.AllKeys.Contains("scanFileOutput_Image") && !string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["scanFileOutput_Image"]) ? ConfigurationManager.AppSettings["scanFileOutput_Image"] : @"images\output\ScannedImages";
        private readonly string _scanFileOutputLoc_pdf = ConfigurationManager.AppSettings.AllKeys.Contains("scanFileOutput_pdf") && !string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["scanFileOutput_pdf"]) ? ConfigurationManager.AppSettings["scanFileOutput_pdf"] : @"images\output\PDF";
        private readonly string _scanResolution = ConfigurationManager.AppSettings.AllKeys.Contains("scanResolution") && !string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["scanResolution"]) ? ConfigurationManager.AppSettings["scanResolution"] : "300";
        private readonly string _scanBrightness = ConfigurationManager.AppSettings.AllKeys.Contains("scanBrightness") && !string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["scanBrightness"]) ? ConfigurationManager.AppSettings["scanBrightness"] : "default_brightness";
        private readonly string _scanContrast = ConfigurationManager.AppSettings.AllKeys.Contains("scanContrast") && !string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["scanContrast"]) ? ConfigurationManager.AppSettings["scanContrast"] : "default_contrast";
        private readonly string _scanColorMode = ConfigurationManager.AppSettings.AllKeys.Contains("scanColorMode") && !string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["scanColorMode"]) ? ConfigurationManager.AppSettings["scanColorMode"] : "2";
        private readonly string _scanAPIUrl = ConfigurationManager.AppSettings.AllKeys.Contains("scanAPIUrl") && !string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["scanAPIUrl"]) ? ConfigurationManager.AppSettings["scanAPIUrl"] : "";


        private readonly string _scanAPIPostMethod = ConfigurationManager.AppSettings.AllKeys.Contains("scanAPIPostMethod") && !string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["scanAPIPostMethod"]) ? ConfigurationManager.AppSettings["scanAPIPostMethod"] : "";
        private readonly string _scanExcludeBlankPages = ConfigurationManager.AppSettings.AllKeys.Contains("scanExcludeBlankPages") && !string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["scanExcludeBlankPages"]) ? ConfigurationManager.AppSettings["scanExcludeBlankPages"] : "0";

        private System.Windows.Forms.Timer _statusTimer;
        private int _statusCounter;

        #region ScannerSetting
        private int iDPI = 300;
        private int iDepthMode = 2;
        private bool excludeBlankPage = false;
        private int iBrighness = 0;//-1000 to 1000
        private int iContrast = 0;//-1000 to 1000
        #endregion

        public frmScanner()
        {
            InitializeComponent();

            // Initialize and configure the Timer
            _statusTimer = new System.Windows.Forms.Timer();
            _statusTimer.Interval = 10000; // 10 seconds
            _statusTimer.Tick += StatusTimer_Tick;

            // Set up
            SetDefaultValue();
            _scanningContext = new ScanningContext(new GdiImageContext());
            _controller = new ScanController(_scanningContext);
        }

        private void SetDefaultValue()
        {
            int.TryParse(_scanResolution, out iDPI);
            iDPI = iDPI < 100 ? 200 : (iDPI > 2400 ? 2400 : iDPI);

            iDepthMode = int.TryParse(_scanColorMode, out iDepthMode) ? iDepthMode : 2; //if _scancolorMode is blank or nothing specified, by default it will be 2;
            iDepthMode = iDepthMode > 2 || iDepthMode < 0 ? 2 : iDepthMode;

            bool.TryParse(_scanExcludeBlankPages, out excludeBlankPage);

            int.TryParse(_scanContrast, out iContrast);
            iContrast = iContrast < -1000 ? -1000 : (iContrast > 1000 ? 1000 : iContrast);
            int.TryParse(_scanBrightness, out iBrighness);
            iBrighness = iBrighness < -1000 ? -1000 : (iBrighness > 1000 ? 1000 : iBrighness);
        }

        private bool ChecksBeforeScan()
        {
            string msg = "";
            if (string.IsNullOrWhiteSpace(_scanAPIUrl))
                msg = "Not able to start scanning, Please provide API url for upoading image after scan..\n connect with application owner for detail";
            else if (string.IsNullOrWhiteSpace(_scanAPIPostMethod))
                msg = "Not able to start scanning, Please provide API method which will handle for upoading image after scan..\n connect with application owner for detail";

            if(!string.IsNullOrWhiteSpace(msg))
            {
                logInformation(LogType.Error, msg);
                MessageBox.Show(msg, "Error -API URL Missing", MessageBoxButtons.OK);
                return false;
            }
            return true;
        }

        private async void RefreshScannerList()
        {
            try
            {
                logInformation(LogType.Info, "Looking for all scanners connected to this system");
                cmbScanners.Items.Clear();
                // Query for available scanning devices
                _devices = await _controller.GetDeviceList();

                foreach (var device in _devices)
                {
                    cmbScanners.Items.Add(device.Name);
                }
                if (cmbScanners.Items.Count > 0)
                {
                    cmbScanners.SelectedIndex = 0; // Select the first scanner by default
                }
                logInformation(LogType.Info, $"{(cmbScanners.Items.Count > 0 ? "Refresh completed, ready for scanning" : " ** No scanning device found attached with this system **")}");
            }
            catch (Exception ex)
            {
                logInformation(LogType.Error, "Something failed while refreshing scanner list");
            }
        }

        private void btnRefresh_Click(object sender, EventArgs e)
        {
            RefreshScannerList();
        }

        private void btnScan_Click(object sender, EventArgs e)
        {
            progressTextBox.Clear();

            logInformation(LogType.Info, "Beginning scan...");
            if (cmbScanners.SelectedIndex == -1)
            {
                MessageBox.Show("Please select a scanner first.");
                return;
            }
            else
            {
                if (ChecksBeforeScan()) Scan(_controller);
            }
        }

        private async void Scan(ScanController controller)
        {
            try
            {
                _statusCounter = 0;
                _statusTimer.Start();


                logInformation(LogType.Info, "    You selected \"cmbScanners.SelectedItem.ToString() \" scanner for document processing");
                var device = _devices.First(f => f.Name == cmbScanners.SelectedItem.ToString());
                
                // Set scanning options
                var options = new ScanOptions
                {
                    Device = device,
                    StretchToPageSize = true,  // Set the desired number of pages to scan
                    PaperSource = (chkDuplexScan.Checked ? PaperSource.Duplex : PaperSource.Auto),
                    FlipDuplexedPages = true,
                    Dpi = iDPI,
                    PageSize = PageSize.A4,
                    BitDepth = (BitDepth)iDepthMode,
                    Brightness =iBrighness,
                    Contrast = iContrast
                };
                // Set the relative directory path where you want to save the files
                string imageSavefullPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, _scanFileOutputLoc_image);

                // Set the relative directory path where you want to save the files
                // Create the directory if it doesn't exist
                if (!Directory.Exists(imageSavefullPath))
                {
                    Directory.CreateDirectory(imageSavefullPath);
                }
                logInformation(LogType.Info, $"    Image file will be saved at {imageSavefullPath} location");

                // Scan and save images
                int i = 1; List<ProcessedImage> images = new List<ProcessedImage>();
                string time = DateTime.Now.ToString("ddMMMyy_hhmmss");
                await foreach (var image in controller.Scan(options))
                {
                    images.Add(image);
                    string fileName = $"{time}_scanned_page_{i++}.png";
                    string filePath = Path.Combine(imageSavefullPath, fileName);
                    image.Save(filePath);

                    logInformation(LogType.Info, $"       Image saved as {fileName}");
                }

                logInformation(LogType.Info, $"    Exporting all scanned Images to PDF format");

                // Set the relative directory path where you want to save the files
                string pdfSavefullPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, _scanFileOutputLoc_pdf);
                // Create the directory if it doesn't exist
                if (!Directory.Exists(pdfSavefullPath))
                {
                    Directory.CreateDirectory(pdfSavefullPath);
                }
                logInformation(LogType.Info, $"        PDF file will be saved at {pdfSavefullPath} location");
                var pdfExporter = new PdfExporter(_scanningContext);
                string pdfFileName = $"scanned_page_{DateTime.Now.ToString("ddMMMyy_hhmmss")}.pdf";
                await pdfExporter.Export(pdfSavefullPath + $"\\{pdfFileName}", images);
                logInformation(LogType.Info, $"    PDF file created {(images.Count > 0 ? "with all image files combined and " : string.Empty)}saved as {pdfFileName}");

                logInformation(LogType.Info, "  ");
                logInformation(LogType.Info, $"    Uploading PDF document on server for future reference");
                if (File.Exists(pdfSavefullPath + $"\\{pdfFileName}"))
                    await UploadScannedImage(_scanFileOutputLoc_pdf + $"\\{pdfFileName}");

                logInformation(LogType.Info, $"Scanning document process completed successfully");
            }
            catch (DeviceWarmingUpException ex) { logInformation(LogType.Error, $"** {ex.Message} **"); }
            catch (DeviceFeederEmptyException ex) { logInformation(LogType.Error, $"** {ex.Message} **"); }
            catch (DeviceBusyException ex) { logInformation(LogType.Error, $"** {ex.Message} **"); }
            catch (DeviceCoverOpenException ex) { logInformation(LogType.Error, $"** {ex.Message} **"); }
            catch (DeviceOfflineException ex) { logInformation(LogType.Error, $"** {ex.Message} **"); }
            catch (DevicePaperJamException ex) { logInformation(LogType.Error, $"** {ex.Message} **"); }
            catch (NoDuplexSupportException ex) { logInformation(LogType.Error, $"** {ex.Message} **"); }
            catch (NoFeederSupportException ex) { logInformation(LogType.Error, $"** {ex.Message} **"); }
            catch (ScanDriverException ex) { logInformation(LogType.Error, $"** {ex.Message} **"); }

            /*
            catch ( ex) { logInformation(LogType.Error, "** No pages are in selected Scanner **"); }
            catch ( ex) { logInformation(LogType.Error, "** Selected Scanner is busy and hence unable to process your request **"); }
            catch ( ex) { logInformation(LogType.Error, "** Selected Scanner Cover is open and hence unable to process your request **"); }
            catch ( ex) { logInformation(LogType.Error, "** Selected Scanner is in offline mode and hence unable to process your request **"); }
            catch ( ex) { logInformation(LogType.Error, "** Selected Scanner has Paper Jam issue and hence unable to process your request **"); }
            catch (NAPS2.Scan.Exceptions.NoDuplexSupportException ex) { "The selected scanner does not support using a feeder. If your scanner does have a feeder, try using a different driver."}
            catch (NAPS2.Scan.Exceptions.NoFeederSupportException ex) { "The selected scanner does not support using a feeder. If your scanner does have a feeder, try using a different driver."}
            catch (NAPS2.Scan.Exceptions.ScanDriverException ex) { "The selected scanner does not support using a feeder. If your scanner does have a feeder, try using a different driver."}
            */

            catch (Exception ex)
            {
                logInformation(LogType.Error, "** Something failed while scanning document **", ex);
            }
            finally { _statusTimer.Stop(); }
        }

        private void frmScanner_Load(object sender, EventArgs e)
        {
            RefreshScannerList();
        }


        private async Task UploadScannedImage(string filePath)
        {
            _statusCounter = 0;
            _statusTimer.Start();

            logInformation(LogType.Info, " ");
            logInformation(LogType.Info, "    Starting PDF file upload to server.. please wait...");
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

                    _statusTimer.Stop();
                }
            }
            catch (Exception ex)
            {
                logInformation(LogType.Error, $"    ** An error occurred while uploading the PDF file: {ex.Message}");
            }
            finally { _statusTimer.Stop(); }
        }

        private void StatusTimer_Tick(object sender, EventArgs e)
        {
            // Update the RichTextBox with a "process running" message
            _statusCounter++;
            logInformation(LogType.Warning, "    Process still running, please wait...");
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
                progressTextBox.SelectionColor = progressTextBox.ForeColor;
                if (_logType == LogType.Error)
                {
                    progressTextBox.SelectionStart = progressTextBox.TextLength;
                    progressTextBox.SelectionLength = 0;

                    // Set the color to red
                    progressTextBox.SelectionColor = Color.Red;
                }
                else if (_logType == LogType.Warning)
                {
                    progressTextBox.SelectionStart = progressTextBox.TextLength;
                    progressTextBox.SelectionLength = 0;

                    // Set the color to red
                    progressTextBox.SelectionColor = Color.DarkBlue;
                }

                progressTextBox.AppendText($"\n{message}");
                progressTextBox.ScrollToCaret();
            }

        }

    }

    enum ScanPageSize
    {
        
    }
}
