using System;
using System.Collections.Generic;
using System.Drawing;
using log4net;
using log4net.Config;
using WIA;
using System.IO;
using Tesseract;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Xml.Linq;
using iText.Kernel.Pdf;
using iText.Layout;
using iText.IO.Image;
using iText.Kernel.Pdf;
using iText.Layout;
using iText.Layout.Element;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Security;
using Image = iText.Layout.Element.Image;
using System.Net.Http;
using System.Configuration;


namespace ScannerService
{
    internal class ScannerService
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(Program));
        private const string ApiBaseUrl = "";
        private readonly string _scanFileFormatID = ConfigurationManager.AppSettings["scanFileFormat"].ToString();
        private readonly string _scanFileOutputLoc = ConfigurationManager.AppSettings["scanFileOutput"].ToString();

        public void SacnnerPages()
        {
            try
            {
                log.Info("Connecting to Scanner...");
                // Connect to scanner
                var deviceManager = new DeviceManager();
                DeviceInfo scanner = null;
                foreach (DeviceInfo deviceInfo in deviceManager.DeviceInfos)
                {
                    if (deviceInfo.Type == WiaDeviceType.ScannerDeviceType)
                    {
                        scanner = deviceInfo;
                        log.Info("  Scanner device found." + scanner.Properties["Name"].get_Value().ToString());
                        break;
                    }
                }

                if (scanner == null)
                {
                    throw new Exception("Scanner not found.");
                }

                // Scan document
                log.Info("  Scanning document...");
                var device = scanner.Connect();
                var item = device.Items[1];
                ConfigureScanner(item); // Set scanner properties for better resolution
                var scannedPages = ScanMultiplePages(item);
                log.Info($" Number of pages scanned: {scannedPages.Count}");

                CombineScannedDocuments(scannedPages);

                /*
                // Process scanned images
                List<string> texts = new List<string>();
                log.Info("Processing scanned images...");
                foreach (var scannedPage in scannedPages)
                {
                    var bitmap = System.Drawing.Image.FromStream((Stream)scannedPage.FileData) as Bitmap;
                    //string text = ProcessImage(bitmap);
                    //texts.Add(text);
                }

                // Output the text
                log.Info("Scanned text:");
                foreach (var text in texts)
                {
                    log.Info(text);
                }
                */
                log.Info("Application finished.");
            }
            catch (Exception ex)
            {
                log.Error($"An error occurred: {ex.Message}");
            }
        }

        static void ConfigureScanner(IItem item)
        {
            try
            {
                // Set scanner properties for better resolution
                var property = item.Properties["Horizontal Resolution"];
                if (property != null)
                {
                    property.set_Value(600); // Set horizontal resolution to 600 DPI
                }

                property = item.Properties["Vertical Resolution"];
                if (property != null)
                {
                    property.set_Value(600); // Set vertical resolution to 600 DPI
                }
            }
            catch (Exception ex)
            {
                log.Error($"Error configuring scanner: {ex.Message}");
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
                    log.Info($"Scanning page {pageNum}...");
                    var imageFile = (ImageFile)item.Transfer(_scanFileFormatID);
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
                log.Error($"Error scanning page: {ex.Message}");
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
            using (PdfWriter writer = new PdfWriter(_scanFileOutputLoc))
            {
                using (PdfDocument pdf = new PdfDocument(writer))
                {
                    Document document = new Document(pdf);
                    foreach (ImageFile imagePath in scannedImagePaths)
                    {
                        //var bitmap =  System.Drawing.Image.FromStream((Stream)imagePath.FileData) as System.Drawing.Image;
                        var bitmap = new Image((iText.Kernel.Pdf.Xobject.PdfImageXObject)imagePath.FileData.ImageFile);
                        document.Add(bitmap);
                    }

                    //foreach (string imagePath in scannedImagePaths)
                    //{
                    //    Image image = new Image(ImageDataFactory.Create(imagePath));
                    //    document.Add(image);
                    //}
                }
            }
        }


        private async Task UploadScannedImage(Bitmap bitmap, HttpClient httpClient)
        {
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
                MessageBox.Show($"An error occurred while uploading the image: {ex.Message}");
            }
        }

    }
}