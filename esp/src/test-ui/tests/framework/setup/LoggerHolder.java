package framework.setup;

import java.util.logging.Logger;

public class LoggerHolder {
    private static Logger errorLogger;
    private static Logger specificLogger;

    public static Logger getErrorLogger() {
        return errorLogger;
    }

    public static void setErrorLogger(Logger errorLogger) {
        LoggerHolder.errorLogger = errorLogger;
    }

    public static Logger getSpecificLogger() {
        return specificLogger;
    }

    public static void setSpecificLogger(Logger specificLogger) {
        LoggerHolder.specificLogger = specificLogger;
    }
}

