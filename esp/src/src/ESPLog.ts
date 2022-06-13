import { LogaccessService, LogLine, GetLogsExRequest } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import * as Observable from "dojo/store/Observable";
import { Paged } from "./store/Paged";
import { BaseStore } from "./store/Store";

const logger = scopedLogger("src/ESPLog.ts");

export const service = new LogaccessService({ baseUrl: "" });

export type LogsQueryStore<T extends GetLogsExRequest> = BaseStore<T, LogLine>;

export function CreateLogsQueryStore<T extends GetLogsExRequest>(): LogsQueryStore<T> {
    const store = new Paged<T, LogLine>({
        start: "LogLineStartFrom",
        count: "LogLineLimit",
    }, "Wuid", request => {
        return service.GetLogsEx(request as any).then(response => {
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
