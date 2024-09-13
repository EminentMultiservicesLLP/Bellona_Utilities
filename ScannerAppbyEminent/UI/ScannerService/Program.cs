using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using log4net;
using log4net.Config;
using System.Windows.Forms;
using System.IO;

namespace ScannerService
{
    internal static class Program
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(Program));
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            // Configure log4net
            XmlConfigurator.Configure(); // Initialize log4net

            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.Run(new Scanner());
        }
    }

    enum LogType
    {
        Info = 1,
        Error = 2,
        Warning
    }

}
