import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { GetLogsExRequest, TargetAudience, LogType } from "@hpcc-js/comms";
import { CreateLogsQueryStore } from "src/ESPLog";
import nlsHPCC from "src/nlsHPCC";
import { useFluentPagedGrid } from "../hooks/grid";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { Filter } from "./forms/Filter";
import { Fields } from "./forms/Fields";
import { ShortVerticalDivider } from "./Common";

const FilterFields: Fields = {
    containerName: { type: "cloud-containername", label: nlsHPCC.ContainerName },
    audience: {
        type: "dropdown", label: nlsHPCC.Audience, options: [
            { key: TargetAudience.Audit, text: "Audit" },
            { key: TargetAudience.Operator, text: "Operator" },
            { key: TargetAudience.Programmer, text: "Programmer" },
            { key: TargetAudience.User, text: "User" }
        ]
    },
    class: {
        type: "dropdown", label: nlsHPCC.Class, options: [
            { key: LogType.Disaster, text: "Audit" },
            { key: LogType.Error, text: "Error" },
            { key: LogType.Information, text: "Information" },
            { key: LogType.Metric, text: "Metric" },
            { key: LogType.Progress, text: "Progress" },
            { key: LogType.Warning, text: "Warning" }
        ]
    },
    jobId: { type: "string", label: nlsHPCC.JobID },
    procId: { type: "string", label: nlsHPCC.ProcessID },
    threadId: { type: "string", label: nlsHPCC.ThreadID },
    message: { type: "string", label: nlsHPCC.Message },
    "StartDate": { type: "datetime", label: nlsHPCC.FromDate },
    "EndDate": { type: "datetime", label: nlsHPCC.ToDate },
};

function formatQuery(_request: any): Partial<GetLogsExRequest> {
    const request: Partial<GetLogsExRequest> = { ..._request };
    if (_request.StartDate) {
        request.StartDate = new Date(_request.StartDate);
    }
    if (_request.EndDate) {
        request.EndDate = new Date(_request.EndDate);
    }
    return request;
}

interface LogsProps {
    wuid?: string;
    filter?: Partial<GetLogsExRequest>;
}
const emptyFilter: Partial<GetLogsExRequest> = {};

export const Logs: React.FunctionComponent<LogsProps> = ({
    wuid,
    filter = emptyFilter,
}) => {

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);
    const [showFilter, setShowFilter] = React.useState(false);

    //  Grid ---
    const gridStore = useConst(CreateLogsQueryStore());

    const query = React.useMemo(() => {
        if (wuid !== undefined) {
            filter.jobId = wuid;
        }
        return formatQuery(filter);
    }, [filter, wuid]);

    const { Grid, GridPagination, refreshTable, copyButtons } = useFluentPagedGrid({
        persistID: "cloudlogs",
        store: gridStore,
        query,
        filename: "logaccess",
        columns: {
            timestamp: { label: nlsHPCC.TimeStamp, width: 140, sortable: false, },
            message: { label: nlsHPCC.Message, sortable: false, },
            containerName: { label: nlsHPCC.ContainerName, width: 100, sortable: false },
            audience: { label: nlsHPCC.Audience, width: 60, sortable: false, },
            class: { label: nlsHPCC.Class, width: 40, sortable: false, },
            jobId: { label: nlsHPCC.JobID, width: 140, sortable: false, hidden: wuid !== undefined, },
            procId: { label: nlsHPCC.ProcessID, width: 46, sortable: false, },
            sequence: { label: nlsHPCC.Sequence, width: 70, sortable: false, },
            threadId: { label: nlsHPCC.ThreadID, width: 60, sortable: false, },
        }
    });

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
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
                <Grid />
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
            </>
        }
        footer={<GridPagination />}
        footerStyles={{}}
    />;
};