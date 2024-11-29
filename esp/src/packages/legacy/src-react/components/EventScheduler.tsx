import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link } from "@fluentui/react";
import { WorkunitsService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { EventScheduleStore } from "src/WsWorkunits";
import nlsHPCC from "src/nlsHPCC";
import * as WsWorkunits from "src/WsWorkunits";
import { useConfirm } from "../hooks/confirm";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { pushParams } from "../util/history";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";
import { PushEventForm } from "./forms/PushEvent";
import { ShortVerticalDivider } from "./Common";
import { QuerySortItem } from "src/store/Store";
import { useMyAccount } from "../hooks/user";

const logger = scopedLogger("src-react/components/EventScheduler.tsx");

const wuService = new WorkunitsService({ baseUrl: "" });

const FilterFields: Fields = {
    "EventName": { type: "string", label: nlsHPCC.EventName, placeholder: nlsHPCC.EventNamePH },
    "JobName": { type: "string", label: nlsHPCC.JobName, placeholder: nlsHPCC.log_analysis_1 },
    "State": { type: "workunit-state", label: nlsHPCC.State, placeholder: "" },
    "Owner": { type: "string", label: nlsHPCC.Owner, placeholder: nlsHPCC.jsmi },
    "EventText": { type: "string", label: nlsHPCC.EventText, placeholder: nlsHPCC.EventTextPH },
    "Cluster": { type: "target-cluster", label: nlsHPCC.Cluster, placeholder: "" },
};

interface EventSchedulerProps {
    filter?: { [key: string]: any };
    sort?: QuerySortItem;
    page?: number;
    store?: EventScheduleStore;
}

const emptyFilter: { [key: string]: any } = {};
const defaultSort = { attribute: "Wuid", descending: true };

export const EventScheduler: React.FunctionComponent<EventSchedulerProps> = ({
    filter = emptyFilter,
    sort = defaultSort,
    store
}) => {

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);

    const [showFilter, setShowFilter] = React.useState(false);
    const [showPushEvent, setShowPushEvent] = React.useState(false);
    const { currentUser } = useMyAccount();
    const {
        selection, setSelection,
        total, setTotal,
        refreshTable } = useFluentStoreState({});

    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            col1: {
                width: 16,
                selectorType: "checkbox"
            },
            Wuid: {
                label: nlsHPCC.Workunit, width: 120,
                formatter: (Wuid, row) => {
                    return <Link href={`#/workunits/${Wuid}`}>{Wuid}</Link>;
                }
            },
            Cluster: { label: nlsHPCC.Cluster, width: 100 },
            JobName: { label: nlsHPCC.JobName, width: 160 },
            EventName: { label: nlsHPCC.EventName, width: 120 },
            EventText: { label: nlsHPCC.EventText, width: 120 },
            Owner: { label: nlsHPCC.Owner, width: 80 },
            State: { label: nlsHPCC.State, width: 60 }
        };
    }, []);

    const refreshData = React.useCallback(() => {
        wuService.WUShowScheduled(filter).then(({ Workunits }) => {
            const workunits = Workunits?.ScheduledWU;
            if (workunits) {
                setData(workunits.map((wu, idx) => {
                    return {
                        Wuid: wu.Wuid,
                        Cluster: wu.Cluster,
                        JobName: wu.JobName,
                        EventName: wu.EventName,
                        EventText: wu.EventText,
                        Owner: wu.Owner,
                        State: wu.State
                    };
                }));
            }
        });
    }, [filter]);

    React.useEffect(() => refreshData(), [refreshData]);

    const copyButtons = useCopyButtons(columns, selection, "events");

    const [DescheduleConfirm, setShowDescheduleConfirm] = useConfirm({
        title: nlsHPCC.Deschedule,
        message: nlsHPCC.DescheduleSelectedWorkunits,
        items: selection.map(s => s.Wuid),
        onSubmit: () => {
            WsWorkunits.WUAction(selection, "Deschedule").then(function (response) {
                refreshTable.call(true);
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
            onClick: () => refreshData()
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
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "mine", text: nlsHPCC.Mine, disabled: !currentUser?.username || !total, iconProps: { iconName: "Contact" }, canCheck: true, checked: filter["Owner"] === currentUser.username,
            onClick: () => {
                if (filter["Owner"] === currentUser.username) {
                    filter["Owner"] = "";
                } else {
                    filter["Owner"] = currentUser.username;
                }
                pushParams(filter);
            }
        },
    ], [currentUser.username, filter, hasFilter, refreshData, selection, setShowDescheduleConfirm, store, total]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <>
                <FluentGrid
                    data={data}
                    primaryID={"Wuid"}
                    sort={sort}
                    columns={columns}
                    setSelection={setSelection}
                    setTotal={setTotal}
                    refresh={refreshTable}
                ></FluentGrid>
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
                <DescheduleConfirm />
                <PushEventForm showForm={showPushEvent} setShowForm={setShowPushEvent} />
            </>
        }
    />;
};
