import * as React from "react";
import { CommandBar, ContextualMenuItemType, DetailsRow, ICommandBarItemProps, IDetailsRowProps, Icon, Image, Link } from "@fluentui/react";
import { hsl as d3Hsl } from "@hpcc-js/common";
import { SizeMe } from "react-sizeme";
import { CreateWUQueryStore, defaultSort, emptyFilter, Get, WUQueryStore, formatQuery } from "src/ESPWorkunit";
import * as WsWorkunits from "src/WsWorkunits";
import { formatCost } from "src/Session";
import { userKeyValStore } from "src/KeyValStore";
import nlsHPCC from "src/nlsHPCC";
import { useConfirm } from "../hooks/confirm";
import { useMyAccount } from "../hooks/user";
import { useUserStore } from "../hooks/store";
import { useLogicalClustersPalette } from "../hooks/platform";
import { calcSearch, pushParams } from "../util/history";
import { useHasFocus, useIsMounted } from "../hooks/util";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentPagedGrid, FluentPagedFooter, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";
import { ShortVerticalDivider } from "./Common";
import { QuerySortItem } from "src/store/Store";

const FilterFields: Fields = {
    "Type": { type: "checkbox", label: nlsHPCC.ArchivedOnly },
    "Protected": { type: "checkbox", label: nlsHPCC.Protected },
    "Wuid": { type: "string", label: nlsHPCC.WUID, placeholder: "W20200824-060035" },
    "Owner": { type: "string", label: nlsHPCC.Owner, placeholder: nlsHPCC.jsmi },
    "Jobname": { type: "string", label: nlsHPCC.JobName, placeholder: nlsHPCC.log_analysis_1 },
    "Cluster": { type: "target-cluster", label: nlsHPCC.Cluster, placeholder: "", multiSelect: true },
    "State": { type: "workunit-state", label: nlsHPCC.State, placeholder: "" },
    "ECL": { type: "string", label: nlsHPCC.ECL, placeholder: nlsHPCC.dataset },
    "LogicalFile": { type: "string", label: nlsHPCC.LogicalFile, placeholder: nlsHPCC.somefile },
    "LogicalFileSearchType": { type: "logicalfile-type", label: nlsHPCC.LogicalFileType, placeholder: "", disabled: (params: Fields) => !params.LogicalFile.value },
    "LastNDays": { type: "string", label: nlsHPCC.LastNDays, placeholder: "2" },
    "StartDate": { type: "datetime", label: nlsHPCC.FromDate },
    "EndDate": { type: "datetime", label: nlsHPCC.ToDate },
};

const defaultUIState = {
    hasSelection: false,
    hasProtected: false,
    hasNotProtected: false,
    hasFailed: false,
    hasNotFailed: false,
    hasCompleted: false,
    hasNotCompleted: false
};

const WORKUNITS_SHOWTIMELINE = "workunits_showTimeline";

export function resetWorkunitOptions() {
    const store = userKeyValStore();
    return store?.delete(WORKUNITS_SHOWTIMELINE);
}

interface WorkunitsProps {
    filter?: { [id: string]: any };
    sort?: QuerySortItem;
    store?: WUQueryStore;
    page?: number;
}

export const Workunits: React.FunctionComponent<WorkunitsProps> = ({
    filter = emptyFilter,
    sort = defaultSort,
    page = 1,
    store
}) => {

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);

    const [showFilter, setShowFilter] = React.useState(false);
    const { currentUser } = useMyAccount();
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [showTimeline, setShowTimeline] = useUserStore<boolean>(WORKUNITS_SHOWTIMELINE, true);
    const {
        selection, setSelection,
        pageNum, setPageNum,
        pageSize, setPageSize,
        total, setTotal,
        refreshTable } = useFluentStoreState({ page });
    const [, , palette] = useLogicalClustersPalette();

    //  Refresh on focus  ---
    const isMounted = useIsMounted();
    const hasFocus = useHasFocus();
    React.useEffect(() => {
        if (isMounted && hasFocus) {
            refreshTable.call();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [hasFocus]);

    //  Grid ---
    const query = React.useMemo(() => {
        return formatQuery(filter);
    }, [filter]);

    const gridStore = React.useMemo(() => {
        return store ? store : CreateWUQueryStore();
    }, [store]);

    const columns = React.useMemo((): FluentColumns => {
        return {
            col1: {
                width: 16,
                selectorType: "checkbox"
            },
            Protected: {
                headerIcon: "LockSolid",
                headerTooltip: nlsHPCC.Protected,
                width: 16,
                sortable: true,
                formatter: (_protected) => {
                    if (_protected === true) {
                        return <Icon iconName="LockSolid" />;
                    }
                    return "";
                }
            },
            Wuid: {
                label: nlsHPCC.WUID, width: 120,
                sortable: true,
                formatter: (Wuid, row) => {
                    const wu = Get(Wuid);
                    const search = calcSearch(filter);
                    return <>
                        <Image src={wu.getStateImage()} styles={{ root: { minWidth: "16px" } }} />
                        &nbsp;
                        <Link href={search ? `#/workunits!${calcSearch(filter)}/${Wuid}` : `#/workunits/${Wuid}`}>{Wuid}</Link >
                    </>;
                }
            },
            Owner: { label: nlsHPCC.Owner, width: 80 },
            Jobname: { label: nlsHPCC.JobName },
            Cluster: { label: nlsHPCC.Cluster },
            RoxieCluster: { label: nlsHPCC.RoxieCluster, sortable: false },
            State: { label: nlsHPCC.State, width: 60 },
            TotalClusterTime: {
                label: nlsHPCC.TotalClusterTime, width: 120,
                justify: "right",
            },
            "Compile Cost": {
                label: nlsHPCC.CompileCost, width: 100,
                justify: "right",
                formatter: (cost, row) => {
                    return `${formatCost(row.CompileCost)}`;
                }
            },
            "Execution Cost": {
                label: nlsHPCC.ExecuteCost, width: 100,
                justify: "right",
                formatter: (cost, row) => {
                    return `${formatCost(row.ExecuteCost)}`;
                }
            },
            "File Access Cost": {
                label: nlsHPCC.FileAccessCost, width: 100,
                justify: "right",
                formatter: (cost, row) => {
                    return `${formatCost(row.FileAccessCost)}`;
                }
            }
        };
    }, [filter]);

    const copyButtons = useCopyButtons(columns, selection, "workunits");

    const doActionWithWorkunits = React.useCallback(async (action: "Delete" | "Abort") => {
        const unknownWUs = selection.filter(wu => wu.State === "unknown");
        if (action === "Delete" && unknownWUs.length) {
            await WsWorkunits.WUAction(unknownWUs, "SetToFailed");
        }
        await WsWorkunits.WUAction(selection, action);
        refreshTable.call(true);
    }, [refreshTable, selection]);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedWorkunits,
        items: selection.map(s => s.Wuid),
        onSubmit: () => doActionWithWorkunits("Delete")
    });

    const [AbortConfirm, setShowAbortConfirm] = useConfirm({
        title: nlsHPCC.Abort,
        message: nlsHPCC.AbortSelectedWorkunits,
        items: selection.map(s => s.Wuid),
        onSubmit: () => doActionWithWorkunits("Abort")
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
            onClick: () => refreshTable.call()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.hasSelection, iconProps: { iconName: "WindowEdit" },
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
            key: "delete", text: nlsHPCC.Delete, disabled: !uiState.hasNotProtected, iconProps: { iconName: "Delete" },
            onClick: () => setShowDeleteConfirm(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "setFailed", text: nlsHPCC.SetToFailed, disabled: !uiState.hasNotProtected,
            onClick: () => { WsWorkunits.WUAction(selection, "SetToFailed").then(() => refreshTable.call()); }
        },
        {
            key: "abort", text: nlsHPCC.Abort, disabled: !uiState.hasNotCompleted,
            onClick: () => setShowAbortConfirm(true)
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "protect", text: nlsHPCC.Protect, disabled: !uiState.hasNotProtected,
            onClick: () => {
                WsWorkunits.WUAction(selection, "Protect").then(() => refreshTable.call());
            }
        },
        {
            key: "unprotect", text: nlsHPCC.Unprotect, disabled: !uiState.hasProtected,
            onClick: () => {
                WsWorkunits.WUAction(selection, "Unprotect").then(() => refreshTable.call());
            }
        },
        { key: "divider_4", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "filter", text: nlsHPCC.Filter, disabled: !!store, iconProps: { iconName: hasFilter ? "FilterSolid" : "Filter" },
            onClick: () => { setShowFilter(true); }
        },
        {
            key: "mine", text: nlsHPCC.Mine, disabled: !currentUser?.username || !total, iconProps: { iconName: "Contact" }, canCheck: true, checked: filter.Owner === currentUser.username,
            onClick: () => {
                if (filter.Owner === currentUser.username) {
                    filter.Owner = "";
                } else {
                    filter.Owner = currentUser.username;
                }
                pushParams(filter);
            }
        },
        { key: "divider_5", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "timeline", text: nlsHPCC.Timeline, canCheck: true, checked: showTimeline, iconProps: { iconName: "TimelineProgress" },
            onClick: () => {
                setShowTimeline(!showTimeline);
                refreshTable.call();
            }
        },
    ], [currentUser.username, filter, hasFilter, refreshTable, selection, setShowAbortConfirm, setShowDeleteConfirm, setShowTimeline, showTimeline, store, total, uiState.hasNotCompleted, uiState.hasNotProtected, uiState.hasProtected, uiState.hasSelection]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        for (let i = 0; i < selection.length; ++i) {
            state.hasSelection = true;
            if (selection[i] && selection[i].Protected !== null) {
                if (selection[i].Protected !== false) {
                    state.hasProtected = true;
                } else {
                    state.hasNotProtected = true;
                }
            }
            if (selection[i] && selection[i].StateID !== null) {
                if (selection[i].StateID === 4) {
                    state.hasFailed = true;
                } else {
                    state.hasNotFailed = true;
                }
                if (WsWorkunits.isComplete(selection[i].StateID, selection[i].ActionEx)) {
                    state.hasCompleted = true;
                } else {
                    state.hasNotCompleted = true;
                }
            }
        }
        setUIState(state);
    }, [selection]);

    const renderRowTimings = React.useCallback((props: IDetailsRowProps, size: { readonly width: number; readonly height: number; }) => {
        if (showTimeline && props) {
            const total = props.item.timings.page.end - props.item.timings.page.start;
            const startPct = 100 - (props.item.timings.start - props.item.timings.page.start) / total * 100;
            const endPct = 100 - (props.item.timings.end - props.item.timings.page.start) / total * 100;
            const backgroundColor = palette(props.item.Cluster);
            const borderColor = d3Hsl(backgroundColor).darker().toString();

            return <div style={{ position: "relative", width: `${size.width - 4}px` }}>
                <DetailsRow {...props} />
                <div style={{
                    position: "absolute",
                    top: 4,
                    bottom: 4,
                    left: `${endPct}%`,
                    width: `${startPct - endPct}%`,
                    backgroundColor,
                    borderColor,
                    borderWidth: 1,
                    borderStyle: "solid",
                    opacity: .33,
                    pointerEvents: "none"
                }} />
            </div>;
        }
        return <DetailsRow {...props} />;
    }, [palette, showTimeline]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <>
                <SizeMe monitorHeight>{({ size }) =>
                    <div style={{ width: "100%", height: "100%" }}>
                        <div style={{ position: "absolute", width: "100%", height: `${size.height}px` }}>
                            <FluentPagedGrid
                                store={gridStore}
                                query={query}
                                sort={sort}
                                pageNum={pageNum}
                                pageSize={pageSize}
                                total={total}
                                columns={columns}
                                height={`${size.height}px`}
                                setSelection={setSelection}
                                setTotal={setTotal}
                                refresh={refreshTable}
                                onRenderRow={showTimeline ? props => renderRowTimings(props, size) : undefined}
                            ></FluentPagedGrid>
                        </div>
                    </div>
                }</SizeMe>
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
                <DeleteConfirm />
                <AbortConfirm />
            </>
        }
        footer={<FluentPagedFooter
            persistID={"workunits"}
            pageNum={pageNum}
            selectionCount={selection.length}
            setPageNum={setPageNum}
            setPageSize={setPageSize}
            total={total}
        ></FluentPagedFooter >}
        footerStyles={{}}
    />;
};
