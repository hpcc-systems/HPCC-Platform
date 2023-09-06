import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { GetLogsExRequest, TargetAudience, LogType } from "@hpcc-js/comms";
import { Level } from "@hpcc-js/util";
import { CreateLogsQueryStore } from "src/ESPLog";
import nlsHPCC from "src/nlsHPCC";
import { logColor } from "src/Utility";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { FluentPagedGrid, FluentPagedFooter, useCopyButtons, useFluentStoreState } from "./controls/Grid";
import { Filter } from "./forms/Filter";
import { Fields } from "./forms/Fields";
import { ShortVerticalDivider } from "./Common";

const maximumTimeUntilRefresh = 8 * 60 * 60 * 1000;
const startTimeOffset = 6 * 60 * 60 * 1000;
const defaultStartDate = new Date(new Date().getTime() - startTimeOffset);

const FilterFields: Fields = {
    containerName: { type: "cloud-containername", label: nlsHPCC.ContainerName },
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
    jobId: { type: "string", label: nlsHPCC.JobID },
    procId: { type: "string", label: nlsHPCC.ProcessID },
    threadId: { type: "string", label: nlsHPCC.ThreadID },
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
    page?: number
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
    page
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
    const gridStore = useConst(CreateLogsQueryStore());

    const query = React.useMemo(() => {
        if (wuid !== undefined) {
            filter.jobId = wuid;
        }
        if (typeof filter.StartDate === "string") {
            filter.StartDate = new Date(filter.StartDate);
        }
        if (filter.StartDate && now.getTime() - filter.StartDate.getTime() > maximumTimeUntilRefresh) {
            filter.StartDate = new Date(now.getTime() - startTimeOffset);
        }
        return formatQuery(filter);
    }, [filter, now, wuid]);

    const columns = React.useMemo(() => {
        return {
            timestamp: { label: nlsHPCC.TimeStamp, width: 140, sortable: false, },
            message: { label: nlsHPCC.Message, sortable: false, },
            containerName: { label: nlsHPCC.ContainerName, width: 100, sortable: false },
            audience: { label: nlsHPCC.Audience, width: 60, sortable: false, },
            class: {
                label: nlsHPCC.Class, width: 40, sortable: false,
                formatter: level => {
                    const colors = logColor(levelMap(level));
                    const styles = { backgroundColor: colors.background, padding: "2px 6px", color: colors.foreground };
                    return <span style={styles}>{level}</span>;
                }
            },
            jobId: { label: nlsHPCC.JobID, width: 140, sortable: false, hidden: wuid !== undefined, },
            procId: { label: nlsHPCC.ProcessID, width: 46, sortable: false, },
            sequence: { label: nlsHPCC.Sequence, width: 70, sortable: false, },
            threadId: { label: nlsHPCC.ThreadID, width: 60, sortable: false, },
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
                    setTotal={setTotal}
                    refresh={refreshTable}
                ></FluentPagedGrid>
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
            </>
        }
        footer={<FluentPagedFooter
            persistID={"cloudlogs"}
            pageNum={pageNum}
            setPageNum={setPageNum}
            setPageSize={setPageSize}
            total={total}
        ></FluentPagedFooter>}
        footerStyles={{}}
    />;
};