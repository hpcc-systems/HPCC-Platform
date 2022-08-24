import { LogaccessService, LogLine, GetLogsExRequest, WsLogaccess } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import * as Observable from "dojo/store/Observable";
import { Paged } from "./store/Paged";
import { BaseStore } from "./store/Store";

const logger = scopedLogger("src/ESPLog.ts");

export const service = new LogaccessService({ baseUrl: "" });

let g_logAccessInfo: Promise<WsLogaccess.GetLogAccessInfoResponse>;
export function GetLogAccessInfo(): Promise<WsLogaccess.GetLogAccessInfoResponse> {
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
        return response.RemoteLogManagerConnectionString !== null || response.RemoteLogManagerType !== null;
    }).catch(e => {
        return false;
    });
}
