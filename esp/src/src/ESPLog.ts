import { WsLogaccess, LogaccessService } from "@hpcc-js/comms";
import * as Observable from "dojo/store/Observable";
import { Paged } from "./store/Paged";
import { BaseStore } from "./store/Store";

const service = new LogaccessService({ baseUrl: "" });

type LogLine = {
    Audience: string;
    Class: string;
    JobId: string;
    Message: string;
    ProcId: number;
    Sequence: string;
    ThreadId: number;
    Timestamp: string;
    ContainerName: string;
};

export type LogsQueryStore = BaseStore<WsLogaccess.GetLogsRequest, LogLine>;

export function CreateLogsQueryStore(): BaseStore<WsLogaccess.GetLogsRequest, LogLine> {
    const store = new Paged<WsLogaccess.GetLogsRequest, LogLine>({
        start: "LogLineStartFrom",
        count: "LogLineLimit",
    }, "Wuid", request => {
        return service.GetLogs(request).then(response => {
            const logLines = JSON.parse(response.LogLines);
            return {
                data: logLines.lines.map(line => {
                    return {
                        Audience: line?.fields[0]["hpcc.log.audience"] ?? "",
                        Class: line?.fields[0]["hpcc.log.class"] ?? "",
                        JobId: line?.fields[0]["hpcc.log.jobid"] ?? "",
                        Message: line?.fields[0]["hpcc.log.message"] ?? "",
                        ProcId: line?.fields[0]["hpcc.log.procid"] ?? "",
                        Sequence: line?.fields[0]["hpcc.log.sequence"] ?? "",
                        ThreadId: line?.fields[0]["hpcc.log.threadid"] ?? "",
                        Timestamp: line?.fields[0]["hpcc.log.timestamp"] ?? "",
                        ContainerName: line?.fields[0]["kubernetes.container.name"] ?? ""
                    };
                }),
                total: logLines.length
            };
        });
    });
    return new Observable(store);
}