import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { GetLogsExRequest, TargetAudience, LogType } from "@hpcc-js/comms";
import { Level } from "@hpcc-js/util";
import { CreateLogsQueryStore } from "src/ESPLog";
import nlsHPCC from "src/nlsHPCC";
import { logColor, wuidToDate, wuidToTime } from "src/Utility";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { FluentPagedGrid, FluentPagedFooter, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { Filter } from "./forms/Filter";
import { Fields } from "./forms/Fields";
import { ShortVerticalDivider } from "./Common";

const eightHours = 8 * 60 * 60 * 1000;
const startTimeOffset = 1 * 60 * 60 * 1000;
const endTimeOffset = 23 * 60 * 60 * 1000;
const defaultStartDate = new Date(new Date().getTime() - startTimeOffset);

const FilterFields: Fields = {
    components: { type: "cloud-containername", label: nlsHPCC.ContainerName },
    instance: { type: "cloud-podname", label: nlsHPCC.PodName },
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

export const Logs: React.FunctionComponent<LogsProps> = ({
    wuid,
    filter = defaultFilter,
    page,
    setLogCount
}) => {

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);
    const [showFilter, setShowFilter] = React.useState(false);
    const {
        selection, setSelection,
        pageNum, setPageNum,
        pageSize, setPageSize,
        total, setTotal,
        refreshTable } = useFluentStoreState({ page });

    const now = React.useMemo(() => new Date(), []);

    //  Grid ---
    const gridStore = useConst(() => CreateLogsQueryStore());

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
            if (!filter.EndDate) {
                filter.EndDate = new Date(now.getTime() + endTimeOffset);
            }
        }
        return formatQuery(filter);
    }, [filter, now, wuid]);

    const columns = React.useMemo((): FluentColumns => {
        return {
            timestamp: { label: nlsHPCC.TimeStamp, width: 140, sortable: false, },
            message: { label: nlsHPCC.Message, sortable: false, },
            components: { label: nlsHPCC.ContainerName, width: 150, sortable: false },
            instance: { label: nlsHPCC.PodName, width: 150, sortable: false },
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
    }, [wuid]);

    const copyButtons = useCopyButtons(columns, selection, "logaccess");

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable.call()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "filter", text: nlsHPCC.Filter, iconProps: { iconName: hasFilter ? "FilterSolid" : "Filter" },
            onClick: () => {
                setShowFilter(true);
            }
        },
    ], [hasFilter, refreshTable]);

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
        return retVal;
    }, [filter, wuid]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <>
                <FluentPagedGrid
                    store={gridStore}
                    query={query}
                    pageNum={pageNum}
                    pageSize={pageSize}
                    total={total}
                    columns={columns}
                    setSelection={setSelection}
                    setTotal={(total) => {
                        setTotal(total);
                        if (setLogCount) {
                            setLogCount(total);
                        }
                    }}
                    refresh={refreshTable}
                ></FluentPagedGrid>
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
            </>
        }
        footer={<FluentPagedFooter
            persistID={"cloudlogs"}
            pageNum={pageNum}
            selectionCount={selection.length}
            setPageNum={setPageNum}
            setPageSize={setPageSize}
            total={total}
        ></FluentPagedFooter>}
        footerStyles={{}}
    />;
};