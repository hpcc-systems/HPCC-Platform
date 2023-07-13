import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import toast from "react-hot-toast";
import { isExceptions } from "@hpcc-js/comms";
import { Dispatch, Level, logger as utilLogger, scopedLogger, Writer, CallbackFunction, Message } from "@hpcc-js/util";
import { CustomToaster } from "../components/controls/CustomToaster";
import * as Utility from "src/Utility";

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
    protected _dispatch = new Dispatch();

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
        const added = this._dispatch.attach(val => callback("added", val));
        return () => {
            added.release();
        };
    }

    doWrite(dateTime: string, level: Level, id: string, message: string): void {
        this._origWriter.write(dateTime, level, id, message);
        const row = { dateTime, level, id, message };
        this._log.push(row);
        toast.custom(CustomToaster({ id, level, message, dateTime }));
        this._dispatch.post(new Message());
    }

    rawWrite(dateTime: string, level: Level, id: string, _msg: string | object): void {
        if (isExceptions(_msg)) {
            _msg.Exception?.forEach(ex => {
                const msg = Utility.decodeHTML(ex.Message);
                this.doWrite(dateTime, level, id, `${ex.Code}: ${msg}`);
            });
        } else {
            if (_msg instanceof Error) {
                _msg = _msg.message;
            } else if (typeof _msg !== "string") {
                _msg = JSON.stringify(_msg, undefined, 2);
            }
            _msg = Utility.decodeHTML(_msg);
            this.doWrite(dateTime, level, id, _msg);
        }
    }
}

export function useECLWatchLogger(): [Readonly<LogEntry[]>, number] {

    const eclLogger = useConst(() => ECLWatchLogger.attach());
    const [lastUpdate, setLastUpdate] = React.useState(Date.now());

    React.useEffect(() => {
        return eclLogger?.listen(() => {
            setLastUpdate(Date.now());
        });
    }, [eclLogger]);

    return [eclLogger.log(), lastUpdate];
}
