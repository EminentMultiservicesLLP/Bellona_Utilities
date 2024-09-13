using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using iTextSharp.text.pdf;
using iTextSharp.text.pdf.parser;
using Tesseract;
using System.Drawing.Imaging;
using System.Text;
using log4net;
using System.Configuration;

namespace ScannerAPI.Controllers
{
    class ExtractText_PDF
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(ExtractText_PDF));
        private static readonly string _tessedit_char_whitelist = ConfigurationManager.AppSettings["tessedit_char_whitelist"].ToString();
        private static readonly string _tessedit_char_blacklist = ConfigurationManager.AppSettings["tessedit_char_blacklist"].ToString();

        #region Methods
        /// <summary>
        /// Extract all images from a pdf, and store them in a list of Images.
        /// </summary>
        /// <param name="PDFSourcePath">Specify PDF Source Path</param>
        /// <returns>List</returns>
        private static List<System.Drawing.Image> ExtractImages(string PDFSourcePath)
        {
            List<System.Drawing.Image> ImgList = new List<System.Drawing.Image>();
            try
            {
                RandomAccessFileOrArray RAFObj = null;
                PdfReader PDFReaderObj = null;
                PdfObject PDFObj = null;
                PdfStream PDFStremObj = null;

                Logging.LogInformation(LogType.Info, "          Start image processing using file stream");
                RAFObj = new RandomAccessFileOrArray(PDFSourcePath);
                Logging.LogInformation(LogType.Info, "          Create new PdfReader object from Stream data");
                PDFReaderObj = new PdfReader(RAFObj, null);

                Logging.LogInformation(LogType.Info, "          Loop all PdfReader object");
                for (int i = 0; i <= PDFReaderObj.XrefSize - 1; i++)
                {
                    PDFObj = PDFReaderObj.GetPdfObject(i);

                    if ((PDFObj != null) && PDFObj.IsStream())
                    {
                        PDFStremObj = (PdfStream)PDFObj;
                        PdfObject subtype = PDFStremObj.Get(PdfName.SUBTYPE);

                        if ((subtype != null) && subtype.ToString() == PdfName.IMAGE.ToString())
                        {
                            try
                            {
                                PdfImageObject PdfImageObj =
                                    new PdfImageObject((PRStream)PDFStremObj);

                                System.Drawing.Image ImgPDF = PdfImageObj.GetDrawingImage();

                                ImgList.Add(ImgPDF);
                            }
                            catch (Exception ex) { /* Fail silently */ Logging.LogInformation(LogType.Error, "          Failing silently while Extracting image from PDF inside for loop", ex); }
                        }
                    }
                }
                Logging.LogInformation(LogType.Info, "          Loop completed for all PdfReader object");
                PDFReaderObj.Close();
                Logging.LogInformation(LogType.Info, "          Close PdfReader object after all processing");
            }
            catch (Exception ex)
            {
                Logging.LogInformation(LogType.Error, "          Failed while Extracting image from PDF", ex);
                throw new Exception(ex.Message);
            }
            return ImgList;
        }

        /// <summary>
        /// Extracts images from a pdf, and saves them to a file.
        /// </summary>
        public static string SaveImages(string filePath, string name, string outputPath, string tessdataPath)
        {
            StringBuilder sText = new StringBuilder();
            try
            {
                Logging.LogInformation(LogType.Info, "      Check if directory for storing image ( to save extract from PDF file) exists, if not then create");
                if (!Directory.Exists(outputPath)) Directory.CreateDirectory(outputPath);

                Logging.LogInformation(LogType.Info, "      Starting ExtractImage and Get a List of Image before reading text from it");
                List<System.Drawing.Image> ListImage = ExtractImages(filePath);
                Logging.LogInformation(LogType.Info, "      Completed ExtractImage and Get a List of Image before reading text from it");

                Logging.LogInformation(LogType.Info, "      Start processing all image list Extracted in previous action");
                for (int i = 0; i < ListImage.Count; i++)
                {
                    try
                    {
                        string currentName = name + i + ".jpg";

                        Bitmap bmpImage = new Bitmap(ListImage[i]);

                        // White out logo
                        using (Graphics graphics = Graphics.FromImage(bmpImage))
                        {
                            graphics.FillRectangle(new SolidBrush(Color.White), 0, 0, 0, 0);
                        }

                        Logging.LogInformation(LogType.Info, "      Start Image Sharpen action");
                        bmpImage = Sharpen(bmpImage);
                        Logging.LogInformation(LogType.Info, "      Complete Image Sharpen action");

                        Logging.LogInformation(LogType.Info, "      Crop Image exactly same size as PDF image");
                        // Crop the image
                        Rectangle cropRect = new Rectangle();
                        cropRect.X = 0;
                        cropRect.Y = 0;
                        cropRect.Width = bmpImage.Width;
                        cropRect.Height = bmpImage.Height;
                        Image croppedimage = bmpImage.Clone(cropRect, bmpImage.PixelFormat);

                        
                        var fileTempPath = System.IO.Path.Combine(outputPath, currentName);
                        Logging.LogInformation(LogType.Info, "      Save the image to a file");
                        croppedimage.Save(fileTempPath, System.Drawing.Imaging.ImageFormat.Jpeg);
                        Logging.LogInformation(LogType.Info, "      Savded image to a file at "+ fileTempPath);

                        Logging.LogInformation(LogType.Info, "      Starting Text Extract process from image file saved at " + fileTempPath);
                        sText.Append(ExtractText(fileTempPath, tessdataPath));
                        Logging.LogInformation(LogType.Info, "      Completed Text Extract process from image file saved");

                    }
                    catch (Exception)
                    { /* Fail silently and continue */ }
                }

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            return sText.ToString();
        }

        /// <summary>
        /// Extracts all the text from an image
        /// </summary>
        /// <param name="pathToImage">The path to the image to extract text from.</param>
        /// <returns>The extracted text</returns>
        private static string ExtractText(string pathToImage, string tessDataPath)
        {
            string sPDFText = string.Empty;
            try
            {
                Logging.LogInformation(LogType.Info, "          Creating the tesseract OCR engine with English as the language :"+tessDataPath);
                using (var tEngine = new TesseractEngine(tessDataPath, "eng", EngineMode.Default))
                {
                    Logging.LogInformation(LogType.Info, "              Setting Tesseract Whitelist & Blacklist variable before extract text");
                    tEngine.SetVariable("tessedit_char_whitelist", _tessedit_char_whitelist);
                    tEngine.SetVariable("tessedit_char_blacklist", _tessedit_char_blacklist);

                    Logging.LogInformation(LogType.Info, "                  Load of the image file from the Pix object which is a wrapper for Leptonica PIX structure");
                    using (var img = Pix.LoadFromFile(pathToImage)) // Load of the image file from the Pix object which is a wrapper for Leptonica PIX structure
                    {
                        Logging.LogInformation(LogType.Info, "                  process the specified image");
                        using (var page = tEngine.Process(img)) //process the specified image
                        {
                            Logging.LogInformation(LogType.Info, "                  Gets the image's content as plain text");
                            sPDFText = page.GetText(); //Gets the image's content as plain text.
                            Logging.LogInformation(LogType.Info, "                  Completed extract image as plain text");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logging.LogInformation(LogType.Error, "                 Unexpected Error while processingn Extract image method : ", ex);
            }
            Logging.LogInformation(LogType.Info, "                 Completed ExtractText method");
            return sPDFText;
        }

        public static Bitmap Sharpen(Bitmap image)
        {
            Bitmap sharpenImage = (Bitmap)image.Clone();

            int filterWidth = 3;
            int filterHeight = 3;
            int width = image.Width;
            int height = image.Height;

            // Create sharpening filter.
            double[,] filter = new double[filterWidth, filterHeight];
            filter[0, 0] = filter[0, 1] = filter[0, 2] = filter[1, 0] = filter[1, 2] = filter[2, 0] = filter[2, 1] = filter[2, 2] = -1;
            filter[1, 1] = 9;

            double factor = 1.0;
            double bias = 0.0;

            Color[,] result = new Color[image.Width, image.Height];

            // Lock image bits for read/write.
            BitmapData pbits = sharpenImage.LockBits(new Rectangle(0, 0, width, height), ImageLockMode.ReadWrite, PixelFormat.Format24bppRgb);

            // Declare an array to hold the bytes of the bitmap.
            int bytes = pbits.Stride * height;
            byte[] rgbValues = new byte[bytes];

            // Copy the RGB values into the array.
            System.Runtime.InteropServices.Marshal.Copy(pbits.Scan0, rgbValues, 0, bytes);

            int rgb;
            // Fill the color array with the new sharpened color values.
            for (int x = 0; x < width; ++x)
            {
                for (int y = 0; y < height; ++y)
                {
                    double red = 0.0, green = 0.0, blue = 0.0;

                    for (int filterX = 0; filterX < filterWidth; filterX++)
                    {
                        for (int filterY = 0; filterY < filterHeight; filterY++)
                        {
                            int imageX = (x - filterWidth / 2 + filterX + width) % width;
                            int imageY = (y - filterHeight / 2 + filterY + height) % height;

                            rgb = imageY * pbits.Stride + 3 * imageX;

                            red += rgbValues[rgb + 2] * filter[filterX, filterY];
                            green += rgbValues[rgb + 1] * filter[filterX, filterY];
                            blue += rgbValues[rgb + 0] * filter[filterX, filterY];
                        }
                        int r = Math.Min(Math.Max((int)(factor * red + bias), 0), 255);
                        int g = Math.Min(Math.Max((int)(factor * green + bias), 0), 255);
                        int b = Math.Min(Math.Max((int)(factor * blue + bias), 0), 255);

                        result[x, y] = Color.FromArgb(r, g, b);
                    }
                }
            }

            // Update the image with the sharpened pixels.
            for (int x = 0; x < width; ++x)
            {
                for (int y = 0; y < height; ++y)
                {
                    rgb = y * pbits.Stride + 3 * x;

                    rgbValues[rgb + 2] = result[x, y].R;
                    rgbValues[rgb + 1] = result[x, y].G;
                    rgbValues[rgb + 0] = result[x, y].B;
                }
            }

            // Copy the RGB values back to the bitmap.
            System.Runtime.InteropServices.Marshal.Copy(rgbValues, 0, pbits.Scan0, bytes);
            // Release image bits.
            sharpenImage.UnlockBits(pbits);

            return sharpenImage;
        }
        #endregion
    }
}