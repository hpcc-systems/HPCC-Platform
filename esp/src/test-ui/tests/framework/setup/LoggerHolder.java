package framework.setup;

import java.util.logging.Logger;

public class LoggerHolder {
    private static Logger logger;

    public static Logger getLogger() {
        return logger;
    }

    public static void setLogger(Logger logger) {
        LoggerHolder.logger = logger;
    }
}

