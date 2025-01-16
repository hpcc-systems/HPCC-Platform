import { Workunit, WorkunitsService, type WsWorkunits } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { Thenable } from "src/store/Deferred";
import { Paged } from "src/store/Paged";
import { BaseStore } from "src/store/Store";
import { wuidToDateTime } from "src/Utility";

const logger = scopedLogger("src-react/comms/workunit.ts");

const service = new WorkunitsService({ baseUrl: "" });

export type WUQueryStore = BaseStore<WsWorkunits.WUQuery, Workunit>;

export function CreateWUQueryStore(): BaseStore<WsWorkunits.WUQuery, Workunit> {
    const store = new Paged<WsWorkunits.WUQuery, Workunit>({
        start: "PageStartFrom",
        count: "PageSize",
        sortBy: "Sortby",
        descending: "Descending"
    }, "Wuid", (request, abortSignal): Thenable<{ data: Workunit[], total: number }> => {
        if (request.Sortby && request.Sortby === "TotalClusterTime") {
            request.Sortby = "ClusterTime";
        }
        return service.WUQuery(request, abortSignal).then(response => {
            const page = {
                start: undefined,
                end: undefined
            };
            const data = response.Workunits.ECLWorkunit.map((wu): Workunit => {
                const start = wuidToDateTime(wu.Wuid);
                if (!page.start || page.start > start) {
                    page.start = start;
                }
                let timePartsSection = 0;
                const end = new Date(start);
                const timeParts = wu.TotalClusterTime?.split(":") ?? [];
                while (timeParts.length) {
                    const timePart = timeParts.pop();
                    switch (timePartsSection) {
                        case 0:
                            end.setSeconds(end.getSeconds() + +timePart);
                            break;
                        case 1:
                            end.setMinutes(end.getMinutes() + +timePart);
                            break;
                        case 2:
                            end.setHours(end.getHours() + +timePart);
                            break;
                        case 3:
                            end.setDate(end.getDate() + +timePart);
                            break;
                    }
                    ++timePartsSection;
                }
                if (!page.end || page.end < end) {
                    page.end = end;
                }
                const retVal = Workunit.attach(service, wu.Wuid, wu);
                //  HPCC-33121 - Move to @hpcc-js/comms  ---
                retVal["__timeline_timings"] = {
                    start,
                    end,
                    page
                };
                return retVal;
            });
            return {
                data,
                total: response.NumWUs
            };
        }).catch(e => {
            if (e.Exception && e.Exception[0] && e.Exception[0].Message === nlsHPCC.GridAbortMessage) {
                logger.debug(e.Exception[0].Message);
            } else {
                logger.error(e);
            }
            return {
                data: [],
                total: 0
            };
        });
    });
    return store;
}
