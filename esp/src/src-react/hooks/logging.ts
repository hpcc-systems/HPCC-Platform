import * as React from "react";
import { useConst, useId } from "@fluentui/react-hooks";
import { useToastController, ToastIntent } from "@fluentui/react-components";
import { isExceptions } from "@hpcc-js/comms";
import { Dispatch, Level, logger as utilLogger, scopedLogger, Writer, CallbackFunction, Message } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import * as Utility from "src/Utility";
import { CustomToaster } from "../components/controls/CustomToaster";

const logger = scopedLogger("../util/logging.ts");

let g_logger: ECLWatchLogger;

class LoggingMessage extends Message {

    readonly dateTime: string;
    readonly level: Level;
    readonly id: string;
    readonly message: string;

    constructor(dateTime: string, level: Level, id: string, message: string) {
        super();
        this.dateTime = dateTime;
        this.level = level;
        this.id = id;
        this.message = message;
    }
}

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
        this._dispatch.post(new LoggingMessage(dateTime, level, id, message));
    }

    rawWrite(dateTime: string, level: Level, id: string, _msg: string | object): void {
        if (isExceptions(_msg)) {
            _msg.Exception?.forEach(ex => {
                const msg = Utility.decodeHTML(ex.Message);
                this.doWrite(dateTime, level, id, `${ex.Code}: ${msg}`);
            });
        } else {
            if (_msg instanceof Error) {
                let errorMessage = _msg.message;
                // include cause if present
                if (_msg.cause) {
                    if (_msg.cause instanceof Error) {
                        errorMessage += `\n\t${nlsHPCC.CausedBy}: "${_msg.cause.message}"`;
                    } else {
                        errorMessage += `\n\t${nlsHPCC.CausedBy}: "${String(_msg.cause)}"`;
                    }
                }
                _msg = errorMessage;
            } else if (typeof _msg !== "string") {
                _msg = JSON.stringify(_msg, undefined, 2);
            }
            _msg = Utility.decodeHTML(_msg);
            this.doWrite(dateTime, level, id, _msg);
        }
    }
}

export function useECLWatchLogger(): { id: string, log: Readonly<LogEntry[]>, lastUpdate: number } {

    const toasterID = useId("logger");
    const { dispatchToast, dismissAllToasts } = useToastController(toasterID);
    const eclLogger = useConst(() => ECLWatchLogger.attach());
    const [lastUpdate, setLastUpdate] = React.useState(Date.now());

    React.useEffect(() => {
        const dispose = eclLogger?.listen((_type: string, messages: LoggingMessage[]) => {
            for (const msg of messages) {
                if (msg.level > Level.info) {
                    let intent: ToastIntent = "info";
                    if (msg.level >= Level.error) {
                        intent = "error";
                    } else if (msg.level >= Level.warning) {
                        intent = "warning";
                    }

                    dispatchToast(CustomToaster({
                        id: msg.id,
                        level: msg.level,
                        message: msg.message,
                        onDismissAll: dismissAllToasts
                    }), { intent });
                }
            }
            setLastUpdate(Date.now());
        });

        return () => {
            dispose?.();
        };
    }, [dismissAllToasts, dispatchToast, eclLogger]);

    return { id: toasterID, log: eclLogger.log(), lastUpdate };
}
