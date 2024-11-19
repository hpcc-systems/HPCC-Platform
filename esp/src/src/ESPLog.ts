import { LogaccessService, LogLine, GetLogsExRequest, WsLogaccess, Exceptions } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import * as Observable from "dojo/store/Observable";
import { Paged } from "./store/Paged";
import { BaseStore } from "./store/Store";

const logger = scopedLogger("src/ESPLog.ts");

export const service = new LogaccessService({ baseUrl: "" });

function isExceptionResponse(response: WsLogaccess.GetLogAccessInfoResponse | { Exceptions?: Exceptions }): response is { Exceptions?: Exceptions } {
    return (response as { Exceptions?: Exceptions }).Exceptions !== undefined;
}

let g_logAccessInfo: Promise<WsLogaccess.GetLogAccessInfoResponse | { Exceptions?: Exceptions }>;
export function GetLogAccessInfo(): Promise<WsLogaccess.GetLogAccessInfoResponse | { Exceptions?: Exceptions }> {
    if (!g_logAccessInfo) {
        g_logAccessInfo = service.GetLogAccessInfo({});
    }
    return g_logAccessInfo;
}

export type LogsQueryStore<T extends GetLogsExRequest> = BaseStore<T, LogLine>;

export function CreateLogsQueryStore<T extends GetLogsExRequest>(): LogsQueryStore<T> {
    const store = new Paged<T, LogLine>({
        start: "LogLineStartFrom",
        count: "LogLineLimit",
    }, "Wuid", request => {
        return Promise.all([GetLogAccessInfo(), service.GetLogsEx(request as any)]).then(([info, response]) => {
            return {
                data: response.lines,
                total: response.total
            };
        }).catch(e => {
            logger.error(e);
            return {
                data: [],
                total: 0
            };
        });
    });
    return new Observable(store);
}

export function hasLogAccess(): Promise<boolean> {
    return GetLogAccessInfo().then(response => {
        if (isExceptionResponse(response)) {
            const err = response.Exceptions.Exception[0].Message;
            logger.info(err);
            return false;
        } else {
            response = response as WsLogaccess.GetLogAccessInfoResponse;
            return response?.RemoteLogManagerConnectionString !== null || response?.RemoteLogManagerType !== null;
        }
    }).catch(e => {
        logger.info(e);
        return false;
    });
}
