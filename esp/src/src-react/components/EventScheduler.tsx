import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import { scopedLogger } from "@hpcc-js/util";
import { CreateEventScheduleStore, EventScheduleStore } from "src/WsWorkunits";
import nlsHPCC from "src/nlsHPCC";
import * as WsWorkunits from "src/WsWorkunits";
import { useConfirm } from "../hooks/confirm";
import { useFluentPagedGrid } from "../hooks/grid";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";
import { PushEventForm } from "./forms/PushEvent";
import { ShortVerticalDivider } from "./Common";
import { QuerySortItem } from "src/store/Store";

const logger = scopedLogger("src-react/components/EventScheduler.tsx");

const FilterFields: Fields = {
    "EventName": { type: "string", label: nlsHPCC.EventName },
    "State": { type: "workunit-state", label: nlsHPCC.State, placeholder: "" },
    "Owner": { type: "string", label: nlsHPCC.Owner, placeholder: nlsHPCC.jsmi },
    "EventText": { type: "string", label: nlsHPCC.EventText, placeholder: nlsHPCC.EventTextPH },
    "Cluster": { type: "target-cluster", label: nlsHPCC.Cluster, placeholder: "" },
};

function formatQuery(_filter) {
    const filter = { ..._filter };
    logger.debug(filter);
    return filter;
}

interface EventSchedulerProps {
    filter?: object;
    sort?: QuerySortItem;
    page?: number;
    store?: EventScheduleStore;
}

const emptyFilter = {};
const defaultSort = { attribute: "Wuid", descending: true };

export const EventScheduler: React.FunctionComponent<EventSchedulerProps> = ({
    filter = emptyFilter,
    sort = defaultSort,
    page = 1,
    store
}) => {

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);

    const [showFilter, setShowFilter] = React.useState(false);
    const [showPushEvent, setShowPushEvent] = React.useState(false);

    //  Grid ---
    const query = React.useMemo(() => {
        return formatQuery(filter);
    }, [filter]);

    const gridStore = React.useMemo(() => {
        return store ? store : CreateEventScheduleStore({});
    }, [store]);

    const { Grid, GridPagination, selection, refreshTable } = useFluentPagedGrid({
        persistID: "events",
        store: gridStore,
        query,
        sort,
        pageNum: page,
        filename: "events",
        columns: {
            col1: {
                width: 16,
                selectorType: "checkbox"
            },
            Wuid: {
                label: nlsHPCC.Workunit, width: 120,
                formatter: React.useCallback(function (Wuid, row) {
                    return <Link href={`#/workunits/${Wuid}`}>{Wuid}</Link>;
                }, [])
            },
            Cluster: { label: nlsHPCC.Cluster, width: 100 },
            JobName: { label: nlsHPCC.JobName, width: 160 },
            EventName: { label: nlsHPCC.EventName, width: 120 },
            EventText: { label: nlsHPCC.EventText, width: 120 },
            Owner: { label: nlsHPCC.Owner, width: 80 },
            State: { label: nlsHPCC.State, width: 60 }
        }
    });

    const [DescheduleConfirm, setShowDescheduleConfirm] = useConfirm({
        title: nlsHPCC.Deschedule,
        message: nlsHPCC.DescheduleSelectedWorkunits,
        items: selection.map(s => s.Wuid),
        onSubmit: () => {
            WsWorkunits.WUAction(selection, "Deschedule").then(function (response) {
                refreshTable();
            }).catch(err => logger.error(err));
        }
    });

    //  Filter  ---
    const filterFields: Fields = {};
    for (const fieldID in FilterFields) {
        filterFields[fieldID] = { ...FilterFields[fieldID], value: filter[fieldID] };
    }

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !selection.length, iconProps: { iconName: "WindowEdit" },
            onClick: () => {
                if (selection.length === 1) {
                    window.location.href = `#/workunits/${selection[0].Wuid}`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/workunits/${selection[i].Wuid}`, "_blank");
                    }
                }
            }
        },
        {
            key: "deschedule", text: nlsHPCC.Deschedule, disabled: !selection.length, iconProps: { iconName: "Delete" },
            onClick: () => setShowDescheduleConfirm(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "filter", text: nlsHPCC.Filter, disabled: !!store, iconProps: { iconName: hasFilter ? "FilterSolid" : "Filter" },
            onClick: () => setShowFilter(true)
        },
        {
            key: "pushEvent", text: nlsHPCC.PushEvent,
            onClick: () => setShowPushEvent(true)
        },
    ], [hasFilter, refreshTable, selection, setShowDescheduleConfirm, store]);

    return <HolyGrail
        header={<CommandBar items={buttons} />}
        main={
            <>
                <SizeMe monitorHeight>{({ size }) =>
                    <div style={{ width: "100%", height: "100%" }}>
                        <div style={{ position: "absolute", width: "100%", height: `${size.height}px` }}>
                            <Grid height={`${size.height}px`} />
                        </div>
                    </div>
                }</SizeMe>
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
                <DescheduleConfirm />
                <PushEventForm showForm={showPushEvent} setShowForm={setShowPushEvent} />
            </>
        }
        footer={<GridPagination />}
        footerStyles={{}}
    />;
};
