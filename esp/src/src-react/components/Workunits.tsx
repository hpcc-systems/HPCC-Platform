import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Icon, Image, Link } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import { scopedLogger } from "@hpcc-js/util";
import { CreateWUQueryStore, Get, WUQueryStore } from "src/ESPWorkunit";
import * as WsWorkunits from "src/WsWorkunits";
import { formatCost } from "src/Session";
import nlsHPCC from "src/nlsHPCC";
import { useConfirm } from "../hooks/confirm";
import { useMyAccount } from "../hooks/user";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { FluentPagedGrid, FluentPagedFooter, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";
import { ShortVerticalDivider } from "./Common";
import { QuerySortItem } from "src/store/Store";

const logger = scopedLogger("src-react/components/Workunits.tsx");

const FilterFields: Fields = {
    "Type": { type: "checkbox", label: nlsHPCC.ArchivedOnly },
    "Protected": { type: "checkbox", label: nlsHPCC.Protected },
    "Wuid": { type: "string", label: nlsHPCC.WUID, placeholder: "W20200824-060035" },
    "Owner": { type: "string", label: nlsHPCC.Owner, placeholder: nlsHPCC.jsmi },
    "Jobname": { type: "string", label: nlsHPCC.JobName, placeholder: nlsHPCC.log_analysis_1 },
    "Cluster": { type: "target-cluster", label: nlsHPCC.Cluster, placeholder: "" },
    "State": { type: "workunit-state", label: nlsHPCC.State, placeholder: "" },
    "ECL": { type: "string", label: nlsHPCC.ECL, placeholder: nlsHPCC.dataset },
    "LogicalFile": { type: "string", label: nlsHPCC.LogicalFile, placeholder: nlsHPCC.somefile },
    "LogicalFileSearchType": { type: "logicalfile-type", label: nlsHPCC.LogicalFileType, placeholder: "", disabled: (params: Fields) => !params.LogicalFile.value },
    "LastNDays": { type: "string", label: nlsHPCC.LastNDays, placeholder: "2" },
    "StartDate": { type: "datetime", label: nlsHPCC.FromDate },
    "EndDate": { type: "datetime", label: nlsHPCC.ToDate },
};

function formatQuery(_filter): { [id: string]: any } {
    const filter = { ..._filter };
    if (filter.LastNDays) {
        const end = new Date();
        const start = new Date();
        start.setDate(end.getDate() - filter.LastNDays);
        filter.StartDate = start.toISOString();
        filter.EndDate = end.toISOString();
        delete filter.LastNDays;
    } else {
        if (filter.StartDate) {
            filter.StartDate = new Date(filter.StartDate).toISOString();
        }
        if (filter.EndDate) {
            filter.EndDate = new Date(filter.EndDate).toISOString();
        }
    }
    if (filter.Type === true) {
        filter.Type = "archived workunits";
    }
    if (filter.Type === true) {
        filter.Type = "archived workunits";
    }
    if (filter.Protected === true) {
        filter.Protected = "Protected";
    }
    logger.debug(filter);
    return filter;
}

const defaultUIState = {
    hasSelection: false,
    hasProtected: false,
    hasNotProtected: false,
    hasFailed: false,
    hasNotFailed: false,
    hasCompleted: false,
    hasNotCompleted: false
};

interface WorkunitsProps {
    filter?: { [id: string]: any };
    sort?: QuerySortItem;
    store?: WUQueryStore;
    page?: number;
}

const emptyFilter: { [id: string]: any } = {};
export const defaultSort = { attribute: "Wuid", descending: true };

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
    const {
        selection, setSelection,
        pageNum, setPageNum,
        pageSize, setPageSize,
        total, setTotal,
        refreshTable } = useFluentStoreState({ page });

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
                formatter: (Wuid, row) => {
                    const wu = Get(Wuid);
                    return <>
                        <Image src={wu.getStateImage()} styles={{ root: { minWidth: "16px" } }} />
                        &nbsp;
                        <Link href={`#/workunits/${Wuid}`}>{Wuid}</Link>
                    </>;
                }
            },
            Owner: { label: nlsHPCC.Owner, width: 80 },
            Jobname: { label: nlsHPCC.JobName },
            Cluster: { label: nlsHPCC.Cluster },
            RoxieCluster: { label: nlsHPCC.RoxieCluster },
            State: { label: nlsHPCC.State, width: 60 },
            TotalClusterTime: {
                label: nlsHPCC.TotalClusterTime, width: 120,
                justify: "right",
            },
            CompileCost: {
                label: nlsHPCC.CompileCost, width: 100,
                justify: "right",
                formatter: (cost, row) => {
                    return `${formatCost(cost)}`;
                }
            },
            ExecuteCost: {
                label: nlsHPCC.ExecuteCost, width: 100,
                justify: "right",
                formatter: (cost, row) => {
                    return `${formatCost(cost)}`;
                }
            },
            FileAccessCost: {
                label: nlsHPCC.FileAccessCost, width: 100,
                justify: "right",
                formatter: (cost, row) => {
                    return `${formatCost(cost)}`;
                }
            }
        };
    }, []);

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
            onClick: () => { WsWorkunits.WUAction(selection, "SetToFailed"); }
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
            key: "mine", text: nlsHPCC.Mine, disabled: !currentUser?.username, iconProps: { iconName: "Contact" }, canCheck: true, checked: filter.Owner === currentUser.username,
            onClick: () => {
                if (filter.Owner === currentUser.username) {
                    filter.Owner = "";
                } else {
                    filter.Owner = currentUser.username;
                }
                pushParams(filter);
            }
        },
    ], [currentUser, filter, hasFilter, refreshTable, selection, setShowAbortConfirm, setShowDeleteConfirm, store, uiState.hasNotCompleted, uiState.hasNotProtected, uiState.hasProtected, uiState.hasSelection]);

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
            setPageNum={setPageNum}
            setPageSize={setPageSize}
            total={total}
        ></FluentPagedFooter>}
        footerStyles={{}}
    />;
};
