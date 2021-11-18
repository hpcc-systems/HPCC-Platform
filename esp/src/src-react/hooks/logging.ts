import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { ESPExceptions, isExceptions } from "@hpcc-js/comms";
import { Observable, Level, logger as utilLogger, scopedLogger, Writer, CallbackFunction } from "@hpcc-js/util";

const logger = scopedLogger("../util/logging.ts");

let g_logger: ECLWatchLogger;

interface LogEntry {
    dateTime: string;
    level: Level;
    id: string;
    message: string;
}

export class ECLWatchLogger implements Writer {

    protected _origWriter: Writer;
    protected _log: LogEntry[] = [];
    protected _observable = new Observable("added");

    static init(): ECLWatchLogger {
        if (!g_logger) {
            g_logger = new ECLWatchLogger();
        } else {
            logger.error("ECLWatchLogger singleton already initialised.");
        }
        return g_logger;
    }

    static attach(): ECLWatchLogger {
        if (!g_logger) {
            logger.error("ECLWatchLogger init not called.");
            g_logger = new ECLWatchLogger();
        }
        return g_logger;
    }

    private constructor() {
        if (location?.search?.split("DEBUG_LOGGING").length > 1) {
            utilLogger.level(Level.debug);
        }
        this._origWriter = utilLogger.writer();
        utilLogger.writer(this);
    }

    log(): Readonly<LogEntry[]> {
        return this._log;
    }

    listen(callback: CallbackFunction): () => void {
        const added = this._observable.addObserver("added", val => callback("added", val));
        return () => {
            added.release();
        };
    }

    doWrite(dateTime: string, level: Level, id: string, message: string): void {
        this._origWriter.write(dateTime, level, id, message);
        const row = { dateTime, level, id, message };
        this._log.push(row);
        this._observable.dispatchEvent("added", row);
    }

    rawWrite(dateTime: string, level: Level, id: string, _msg: string | object): void {
        if (_msg instanceof ESPExceptions) {
            this.doWrite(dateTime, level, id, _msg.message);
        } else if (isExceptions(_msg)) {
            _msg.Exception?.forEach(ex => {
                this.doWrite(dateTime, level, "" + ex.Code, ex.Message);
            });
        } else if (_msg instanceof Error) {
            this.doWrite(dateTime, level, id, _msg.message);
        } else if (typeof _msg !== "string") {
            this.doWrite(dateTime, level, id, JSON.stringify(_msg, undefined, 2));
        } else if (typeof _msg === "string") {
            this.doWrite(dateTime, level, id, _msg);
        }
    }
}

export function useECLWatchLogger(): [Readonly<LogEntry[]>, number] {

    const eclLogger = useConst(() => ECLWatchLogger.attach());
    const [lastUpdate, setLastUpdate] = React.useState(Date.now());

    React.useEffect(() => {
        return eclLogger?.listen((eventID, row) => {
            setLastUpdate(Date.now());
        });
    }, [eclLogger]);

    return [eclLogger.log(), lastUpdate];
}
