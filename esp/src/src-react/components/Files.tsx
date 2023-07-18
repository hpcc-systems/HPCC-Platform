import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Icon, Link } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import * as WsDfu from "src/WsDfu";
import { CreateDFUQueryStore } from "src/ESPLogicalFile";
import { formatCost } from "src/Session";
import * as Utility from "src/Utility";
import { QuerySortItem } from "src/store/Store";
import nlsHPCC from "src/nlsHPCC";
import { useConfirm } from "../hooks/confirm";
import { useFluentPagedGrid } from "../hooks/grid";
import { useMyAccount } from "../hooks/user";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { AddToSuperfile } from "./forms/AddToSuperfile";
import { CopyFile } from "./forms/CopyFile";
import { DesprayFile } from "./forms/DesprayFile";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";
import { RemoteCopy } from "./forms/RemoteCopy";
import { RenameFile } from "./forms/RenameFile";
import { ShortVerticalDivider } from "./Common";
import { SizeMe } from "react-sizeme";

const logger = scopedLogger("src-react/components/Files.tsx");

const FilterFields: Fields = {
    "LogicalName": { type: "string", label: nlsHPCC.Name, placeholder: nlsHPCC.somefile },
    "Description": { type: "string", label: nlsHPCC.Description, placeholder: nlsHPCC.SomeDescription },
    "Owner": { type: "string", label: nlsHPCC.Owner, placeholder: nlsHPCC.jsmi },
    "LogicalFiles": { type: "checkbox", label: nlsHPCC.LogicalFiles },
    "SuperFiles": { type: "checkbox", label: nlsHPCC.SuperFiles },
    "Indexes": { type: "checkbox", label: nlsHPCC.Indexes },
    "NotInSuperfiles": { type: "checkbox", label: nlsHPCC.NotInSuperfiles },
    "NodeGroup": { type: "target-group", label: nlsHPCC.Group, placeholder: nlsHPCC.Cluster },
    "FileSizeFrom": { type: "string", label: nlsHPCC.FromSizes, placeholder: "4096" },
    "FileSizeTo": { type: "string", label: nlsHPCC.ToSizes, placeholder: "16777216" },
    "FileType": { type: "file-type", label: nlsHPCC.FileType },
    "FirstN": { type: "string", label: nlsHPCC.FirstN, placeholder: "-1" },
    // "Sortby": { type: "file-sortby", label: nlsHPCC.FirstNSortBy, disabled: (params: Fields) => !params.FirstN.value },
    "StartDate": { type: "datetime", label: nlsHPCC.FromDate },
    "EndDate": { type: "datetime", label: nlsHPCC.ToDate },
};

function formatQuery(_filter): { [id: string]: any } {
    const filter = { ..._filter };
    if (filter.LogicalFiles || filter.SuperFiles) {
        filter.FileType = "";
        if (!filter.Indexes) {
            filter.ContentType = "key";
            filter.InvertContent = true;
        }
        if (filter.LogicalFiles && !filter.SuperFiles) {
            filter.FileType = "Logical Files Only";
        } else if (!filter.LogicalFiles && filter.SuperFiles) {
            filter.FileType = "Superfiles Only";
        }
    } else if (filter.NotInSuperfiles) {
        filter.FileType = "Not in Superfiles";
        if (!filter.Indexes) {
            filter.ContentType = "key";
            filter.InvertContent = true;
        }
    } else if (filter.Indexes) {
        filter.ContentType = "key";
    }
    delete filter.LogicalFiles;
    delete filter.SuperFiles;
    delete filter.NotInSuperFiles;
    delete filter.Indexes;
    if (filter.StartDate) {
        filter.StartDate = new Date(filter.StartDate).toISOString();
    }
    if (filter.EndDate) {
        filter.EndDate = new Date(filter.StartDate).toISOString();
    }
    return filter;
}

const defaultUIState = {
    hasSelection: false,
};

interface FilesProps {
    filter?: { [id: string]: any };
    sort?: QuerySortItem;
    store?: any;
    page?: number;
}

const emptyFilter = {};
export const defaultSort = { attribute: "Modified", descending: true };

export const Files: React.FunctionComponent<FilesProps> = ({
    filter = emptyFilter,
    sort = defaultSort,
    page = 1,
    store
}) => {

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);

    const [showFilter, setShowFilter] = React.useState(false);
    const [showRemoteCopy, setShowRemoteCopy] = React.useState(false);
    const [showCopy, setShowCopy] = React.useState(false);
    const [showRenameFile, setShowRenameFile] = React.useState(false);
    const [showAddToSuperfile, setShowAddToSuperfile] = React.useState(false);
    const [showDesprayFile, setShowDesprayFile] = React.useState(false);
    const { currentUser } = useMyAccount();
    const [viewByScope, setViewByScope] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Grid ---
    const gridStore = React.useMemo(() => {
        return store ? store : CreateDFUQueryStore();
    }, [store]);

    const query = React.useMemo(() => {
        return formatQuery(filter);
    }, [filter]);

    const { Grid, GridPagination, selection, refreshTable, copyButtons } = useFluentPagedGrid({
        persistID: "files",
        store: gridStore,
        query,
        sort,
        pageNum: page,
        filename: "logicalfiles",
        columns: {
            col1: {
                width: 16,
                disabled: React.useCallback(function (item) {
                    return item ? item.__hpcc_isDir : true;
                }, []),
                selectorType: "checkbox"
            },
            IsProtected: {
                headerIcon: "LockSolid",
                headerTooltip: nlsHPCC.Protected,
                width: 16,
                sortable: false,
                formatter: React.useCallback(function (_protected) {
                    if (_protected === true) {
                        return <Icon iconName="LockSolid" />;
                    }
                    return "";
                }, [])
            },
            IsCompressed: {
                headerIcon: "ZipFolder",
                headerTooltip: nlsHPCC.Compressed,
                width: 16,
                sortable: false,
                formatter: React.useCallback(function (compressed) {
                    if (compressed === true) {
                        return <Icon iconName="ZipFolder" />;
                    }
                    return "";
                }, [])
            },
            Name: {
                label: nlsHPCC.LogicalName,
                formatter: React.useCallback(function (name, row) {
                    if (row.__hpcc_isDir) {
                        return name;
                    }
                    const url = "#/files/" + (row.NodeGroup ? row.NodeGroup + "/" : "") + name;
                    return <>
                        <Icon iconName={row.getStateIcon ? row.getStateIcon() : ""} />
                        &nbsp;
                        <Link href={url}>{name}</Link>
                    </>;
                }, []),
            },
            Owner: { label: nlsHPCC.Owner },
            SuperOwners: { label: nlsHPCC.SuperOwner },
            Description: { label: nlsHPCC.Description },
            NodeGroup: { label: nlsHPCC.Cluster },
            RecordCount: {
                label: nlsHPCC.Records,
                formatter: React.useCallback(function (value, row) {
                    return Utility.valueCleanUp(value);
                }, []),
            },
            IntSize: {
                label: nlsHPCC.Size,
                formatter: React.useCallback(function (value, row) {
                    return Utility.convertedSize(value);
                }, []),
            },
            Parts: {
                label: nlsHPCC.Parts, width: 40,
                formatter: React.useCallback(function (value, row) {
                    return Utility.valueCleanUp(value);
                }, []),
            },
            MinSkew: {
                label: nlsHPCC.MinSkew, width: 60, formatter: React.useCallback((value, row) => value ? `${Utility.formatDecimal(value / 100)}%` : "", [])
            },
            MaxSkew: {
                label: nlsHPCC.MaxSkew, width: 60, formatter: React.useCallback((value, row) => value ? `${Utility.formatDecimal(value / 100)}%` : "", [])
            },
            Modified: { label: nlsHPCC.ModifiedUTCGMT },
            AtRestCost: {
                label: nlsHPCC.FileCostAtRest,
                formatter: React.useCallback(function (cost, row) {
                    return `${formatCost(cost)}`;
                }, [])
            },
            AccessCost: {
                label: nlsHPCC.FileAccessCost,
                formatter: React.useCallback(function (cost, row) {
                    return `${formatCost(cost)}`;
                }, [])
            }
        }
    });

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedFiles,
        items: selection.map(s => s.Name),
        onSubmit: React.useCallback(() => {
            WsDfu.DFUArrayAction(selection, "Delete")
                .then(({ DFUArrayActionResponse }) => {
                    const ActionResults = DFUArrayActionResponse?.ActionResults?.DFUActionInfo ?? [];
                    ActionResults.filter(action => action?.Failed).forEach(action => logger.error(action?.ActionResult));
                    refreshTable(true);
                })
                .catch(err => logger.error(err));
        }, [refreshTable, selection])
    });

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.hasSelection, iconProps: { iconName: "WindowEdit" },
            onClick: () => {
                if (selection.length === 1) {
                    window.location.href = "#/files/" + (selection[0].NodeGroup ? selection[0].NodeGroup + "/" : "") + selection[0].Name;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open("#/files/" + (selection[i].NodeGroup ? selection[i].NodeGroup + "/" : "") + selection[i].Name, "_blank");
                    }
                }
            }
        },
        {
            key: "delete", text: nlsHPCC.Delete, disabled: !uiState.hasSelection, iconProps: { iconName: "Delete" },
            onClick: () => setShowDeleteConfirm(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "remoteCopy", text: nlsHPCC.RemoteCopy,
            onClick: () => setShowRemoteCopy(true)
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "copy", text: nlsHPCC.Copy, disabled: !uiState.hasSelection,
            onClick: () => setShowCopy(true)
        },
        {
            key: "rename", text: nlsHPCC.Rename, disabled: !uiState.hasSelection,
            onClick: () => setShowRenameFile(true)
        },
        {
            key: "addToSuperfile", text: nlsHPCC.AddToSuperfile, disabled: !uiState.hasSelection,
            onClick: () => setShowAddToSuperfile(true)
        },
        {
            key: "despray", text: nlsHPCC.Despray, disabled: !uiState.hasSelection,
            onClick: () => setShowDesprayFile(true)
        },
        { key: "divider_4", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "filter", text: nlsHPCC.Filter, disabled: !!store, iconProps: { iconName: hasFilter ? "FilterSolid" : "Filter" },
            onClick: () => {
                setShowFilter(true);
            }
        },
        {
            key: "viewByScope", text: nlsHPCC.ViewByScope, iconProps: { iconName: "BulletedTreeList" }, iconOnly: true, canCheck: true, checked: viewByScope,
            onClick: () => {
                setViewByScope(!viewByScope);
                window.location.href = "#/scopes";
            }
        },
        { key: "divider_5", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "mine", text: nlsHPCC.Mine, disabled: !currentUser?.username, iconProps: { iconName: "Contact" }, canCheck: true, checked: filter["Owner"] === currentUser.username,
            onClick: () => {
                if (filter["Owner"] === currentUser.username) {
                    filter["Owner"] = "";
                } else {
                    filter["Owner"] = currentUser.username;
                }
                pushParams(filter);
            }
        },
    ], [currentUser, filter, hasFilter, refreshTable, selection, setShowDeleteConfirm, store, uiState.hasSelection, viewByScope]);

    //  Filter  ---
    const filterFields: Fields = {};
    for (const field in FilterFields) {
        filterFields[field] = { ...FilterFields[field], value: filter[field] };
    }

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        for (let i = 0; i < selection.length; ++i) {
            state.hasSelection = true;
            //  TODO:  More State
        }
        setUIState(state);
    }, [selection]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <>
                <SizeMe monitorHeight>{({ size }) =>
                    <div style={{ position: "relative", width: "100%", height: "100%" }}>
                        <div style={{ position: "absolute", width: "100%", height: `${size.height}px` }}>
                            <Grid height={`${size.height}px`} />
                        </div>
                    </div>
                }</SizeMe>
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
                <RemoteCopy showForm={showRemoteCopy} setShowForm={setShowRemoteCopy} refreshGrid={refreshTable} />
                <CopyFile logicalFiles={selection.map(s => s.Name)} showForm={showCopy} setShowForm={setShowCopy} refreshGrid={refreshTable} />
                <RenameFile logicalFiles={selection.map(s => s.Name)} showForm={showRenameFile} setShowForm={setShowRenameFile} refreshGrid={refreshTable} />
                <AddToSuperfile logicalFiles={selection.map(s => s.Name)} showForm={showAddToSuperfile} setShowForm={setShowAddToSuperfile} refreshGrid={refreshTable} />
                <DesprayFile logicalFiles={selection.map(s => s.Name)} showForm={showDesprayFile} setShowForm={setShowDesprayFile} />
                <DeleteConfirm />
            </>
        }
        footer={<GridPagination />}
        footerStyles={{}}
    />;
};
