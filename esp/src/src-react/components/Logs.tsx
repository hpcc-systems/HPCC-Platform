import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { GetLogsExRequest, LogaccessService, LogType, TargetAudience, WsLogaccess } from "@hpcc-js/comms";
import { Level, scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { formatDateString, logColor, removeAllExcept, timestampToDate, wuidToDate, wuidToTime } from "src/Utility";
import { useLogAccessInfo } from "../hooks/platform";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { Filter } from "./forms/Filter";
import { Fields } from "./forms/Fields";
import { ShortVerticalDivider } from "./Common";

export const service = new LogaccessService({ baseUrl: "" });

const logger = scopedLogger("src-react/components/Logs.tsx");

const eightHours = 8 * 60 * 60 * 1000;
const startTimeOffset = 1 * 60 * 60 * 1000;
const endTimeOffset = 23 * 60 * 60 * 1000;
const defaultStartDate = new Date(new Date().getTime() - startTimeOffset);

const FilterFields: Fields = {
    components: { type: "cloud-containername", label: nlsHPCC.ContainerName },
    audience: {
        type: "dropdown", label: nlsHPCC.Audience, options: [
            { key: TargetAudience.Operator, text: "Operator" },
            { key: TargetAudience.User, text: "User" },
            { key: TargetAudience.Programmer, text: "Programmer" },
            { key: TargetAudience.Audit, text: "Audit" },
        ]
    },
    class: {
        type: "dropdown-multi", label: nlsHPCC.Class, options: [
            { key: LogType.Disaster, text: "Disaster" },
            { key: LogType.Error, text: "Error" },
            { key: LogType.Warning, text: "Warning" },
            { key: LogType.Information, text: "Information" },
            { key: LogType.Progress, text: "Progress" },
            { key: LogType.Metric, text: "Metric" },
        ]
    },
    workunits: { type: "string", label: nlsHPCC.JobID },
    processid: { type: "string", label: nlsHPCC.ProcessID },
    threadid: { type: "string", label: nlsHPCC.ThreadID },
    message: { type: "string", label: nlsHPCC.Message },
    LogLineLimit: {
        type: "dropdown", label: nlsHPCC.LogLineLimit, options: [
            { key: 100, text: "100" },
            { key: 250, text: "250" },
            { key: 500, text: "500" },
            { key: 1000, text: "1000" },
        ]
    },
    StartDate: { type: "datetime", label: nlsHPCC.FromDate },
    EndDate: { type: "datetime", label: nlsHPCC.ToDate },
};

function formatQuery(_request: any): Partial<GetLogsExRequest> {
    const request: Partial<GetLogsExRequest> = { ..._request };
    if (_request.StartDate) {
        request.StartDate = new Date(_request.StartDate);
    }
    if (_request.EndDate) {
        request.EndDate = new Date(_request.EndDate);
    }
    if (_request.class) {
        request.class = _request.class.split(",");
    }
    return request;
}

interface LogsProps {
    wuid?: string;
    filter?: Partial<GetLogsExRequest>;
    page?: number;
    setLogCount?: (count: number | string) => void;
}
export const defaultFilter: Partial<GetLogsExRequest> = { StartDate: defaultStartDate };

const levelMap = (level) => {
    switch (level) {
        case "ERR":
            return Level.error;
        case "WRN":
            return Level.warning;
        case "PRO":
            return Level.debug;
        case "INF":
        default:
            return Level.info;
    }
};

const columnOrder: string[] = [WsLogaccess.LogColumnType.timestamp, WsLogaccess.LogColumnType.message];

export const Logs: React.FunctionComponent<LogsProps> = ({
    wuid,
    filter = defaultFilter,
    page,
    setLogCount
}) => {

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);
    const [showFilter, setShowFilter] = React.useState(false);
    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({ page });

    const now = React.useMemo(() => new Date(), []);

    const { columns: logColumns } = useLogAccessInfo();

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        // we've defined the columnOrder array above to ensure specific columns will
        // appear on the left-most side of the grid, eg timestamps and log messages
        const cols = logColumns?.sort((a, b) => {
            const logTypeA = columnOrder.indexOf(a.LogType);
            const logTypeB = columnOrder.indexOf(b.LogType);

            if (logTypeA >= 0) {
                if (logTypeB >= 0) { return logTypeA - logTypeB; }
                return -1;
            } else if (logTypeB >= 0) {
                return 1;
            } else {
                return 0;
            }
        });
        const retVal = {
            timestamp: {
                label: nlsHPCC.TimeStamp, width: 140, sortable: false,
                formatter: ts => {
                    if (ts) {
                        if (ts.indexOf(":") < 0) {
                            return timestampToDate(ts).toISOString();
                        }
                        return formatDateString(ts);
                    }
                },
            },
            message: { label: nlsHPCC.Message, width: 600, sortable: false, },
            components: { label: nlsHPCC.ContainerName, width: 150, sortable: false },
            audience: { label: nlsHPCC.Audience, width: 60, sortable: false, },
            class: {
                label: nlsHPCC.Class, width: 40, sortable: false,
                formatter: level => {
                    const colors = logColor(levelMap(level));
                    const styles = { backgroundColor: colors.background, padding: "2px 6px", color: colors.foreground };
                    return <span style={styles}>{level}</span>;
                }
            },
            workunits: { label: nlsHPCC.JobID, width: 50, sortable: false, hidden: wuid !== undefined, },
            processid: { label: nlsHPCC.ProcessID, width: 75, sortable: false, },
            logid: { label: nlsHPCC.Sequence, width: 70, sortable: false, },
            threadid: { label: nlsHPCC.ThreadID, width: 60, sortable: false, },
        };
        const colTypes = cols?.map(c => c.LogType.toString()) ?? [];
        removeAllExcept(retVal, colTypes);
        return retVal;
    }, [logColumns, wuid]);

    const copyButtons = useCopyButtons(columns, selection, "logaccess");

    const query = React.useMemo(() => {
        if (wuid !== undefined) {
            filter.workunits = wuid;
            if (typeof filter.StartDate === "string") {
                filter.StartDate = new Date(filter.StartDate + ":00Z");
            } else {
                filter.StartDate = new Date(`${wuidToDate(wuid)}T${wuidToTime(wuid)}Z`);
            }
        } else {
            if (typeof filter.StartDate === "string") {
                filter.StartDate = new Date(filter.StartDate + ":00Z");
            }
            if (!filter.StartDate) {
                //assign a reasonable default start date if one isn't set
                filter.StartDate = new Date(now.getTime() - eightHours);
            }
            if (typeof filter.EndDate === "string") {
                filter.EndDate = new Date(filter.EndDate + ":00Z");
            }
            if (!filter.EndDate) {
                filter.EndDate = new Date(now.getTime() + endTimeOffset);
            }
        }
        return formatQuery(filter);
    }, [filter, now, wuid]);

    const refreshData = React.useCallback(() => {
        service.GetLogsEx(query as any).then(response => {
            setData(response.lines);
        }).catch(err => logger.error(err));
    }, [query]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "filter", text: nlsHPCC.Filter, iconProps: { iconName: hasFilter ? "FilterSolid" : "Filter" },
            onClick: () => {
                setShowFilter(true);
            }
        },
    ], [hasFilter, refreshData]);

    React.useEffect(() => {
        refreshData();
    }, [refreshData]);

    //  Filter  ---
    const filterFields: Fields = React.useMemo(() => {
        const retVal: Fields = {};
        for (const field in FilterFields) {
            retVal[field] = { ...FilterFields[field], value: filter[field] };
            if (wuid !== undefined) {
                delete filter.workunits;
                delete retVal.jobId;
            }
        }
        const colTypes = logColumns?.map(c => c.LogType.toString()) ?? [];
        removeAllExcept(retVal, colTypes);
        return retVal;
    }, [filter, logColumns, wuid]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <div style={{ position: "relative", height: "100%" }}>
                <FluentGrid
                    data={data}
                    primaryID={""}
                    columns={columns}
                    setSelection={setSelection}
                    setTotal={(total) => {
                        setTotal(total);
                        if (setLogCount) {
                            setLogCount(total);
                        }
                    }}
                    refresh={refreshTable}
                ></FluentGrid>
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
            </div>
        }
    />;
};