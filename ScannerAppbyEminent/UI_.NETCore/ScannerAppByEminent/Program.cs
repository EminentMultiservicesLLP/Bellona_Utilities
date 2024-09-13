using log4net;
using log4net.Config;

namespace ScannerApp
{
    internal static class Program
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(Program));

        /// <summary>
        ///  The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            XmlConfigurator.Configure(); // Initialize log4net
            ApplicationConfiguration.Initialize();
            Application.Run(new frmScanner());
        }
    }
    enum LogType
    {
        Info = 1,
        Error = 2,
        Warning
    }
}