using log4net;
using System;

namespace ScannerAPI
{
    public enum LogType
    {
        Info = 1,
        Error = 2,
        Warning
    }

    public class Logging
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(Logging));
        public static void LogInformation(LogType _logType, string message, Exception ex = null)
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
        }
    }
}