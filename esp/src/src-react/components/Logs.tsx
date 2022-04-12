import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { LogsQueryStore, CreateLogsQueryStore } from "src/ESPLog";
import { useFluentPagedGrid } from "../hooks/grid";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { Filter } from "./forms/Filter";
import { Fields } from "./forms/Fields";
import nlsHPCC from "src/nlsHPCC";
import { ShortVerticalDivider } from "./Common";

const FilterFields: Fields = {
    "LogCategory": {
        type: "dropdown", label: nlsHPCC.Category,
        options: [
            { key: "0", text: "All" },
            { key: "1", text: "WorkunitID" },
            { key: "2", text: "Component" },
            { key: "3", text: "Log Type" },
            { key: "4", text: "Target Audience" },
            { key: "5", text: "Source Instance" },
            { key: "6", text: "Source Node" },
        ]
    },
    "SearchByValue": { type: "string", label: nlsHPCC.Value, placeholder: nlsHPCC.somefile },
    "StartDate": { type: "datetime", label: nlsHPCC.FromDate },
    "EndDate": { type: "datetime", label: nlsHPCC.ToDate },
};

function formatQuery(_filter) {
    const filter = { ..._filter };
    if (!filter.LogCategory) {
        filter.LogCategory = "0";
    }
    filter.Range = {};
    if (filter.StartDate) {
        filter.Range.StartDate = new Date(filter.StartDate).toISOString();
        delete filter.StartDate;
    } else {
        filter.Range.StartDate = new Date().toISOString();
    }
    if (filter.EndDate) {
        filter.Range.EndDate = new Date(filter.EndDate).toISOString();
        delete filter.EndDate;
    }
    filter.Format = "JSON";
    return filter;
}

interface LogsProps {
    filter?: object;
    store?: LogsQueryStore;
}
const emptyFilter = {};

export const Logs: React.FunctionComponent<LogsProps> = ({
    filter = emptyFilter,
    store
}) => {

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);

    const [showFilter, setShowFilter] = React.useState(false);

    //  Grid ---
    const gridStore = React.useMemo(() => {
        return store ? store : CreateLogsQueryStore();
    }, [store]);

    const query = React.useMemo(() => {
        return formatQuery(filter);
    }, [filter]);

    const { Grid, GridPagination, refreshTable, copyButtons } = useFluentPagedGrid({
        persistID: "cloudlogs",
        store: gridStore,
        query,
        filename: "logaccess",
        columns: {
            Timestamp: { label: nlsHPCC.TimeStamp, width: 140, },
            Message: { label: "Message", },
            ContainerName: { label: "Container Name", width: 100 },
            Audience: { label: "Audience", width: 60, },
            Class: { label: "Class", width: 40, },
            JobId: { label: "JobId", width: 40, sortable: false, },
            ProcId: { label: "ProcId", width: 46, },
            Sequence: { label: "Sequence", width: 70, sortable: false, },
            ThreadId: { label: "ThreadId", width: 60, },
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
            key: "filter", text: nlsHPCC.Filter, disabled: !!store, iconProps: { iconName: hasFilter ? "FilterSolid" : "Filter" },
            onClick: () => {
                setShowFilter(true);
            }
        },
    ], [hasFilter, refreshTable, store]);

    //  Filter  ---
    const filterFields: Fields = {};
    for (const field in FilterFields) {
        filterFields[field] = { ...FilterFields[field], value: filter[field] };
    }

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