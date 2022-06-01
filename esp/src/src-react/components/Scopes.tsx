import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Icon, Link, mergeStyleSets } from "@fluentui/react";
import { DFUService } from "@hpcc-js/comms";
import { SizeMe } from "react-sizeme";
import * as WsDfu from "src/WsDfu";
import { formatCost } from "src/Session";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useConfirm } from "../hooks/confirm";
import { useFluentGrid } from "../hooks/grid";
import { useBuildInfo } from "../hooks/platform";
import { useUserTheme } from "../hooks/theme";
import { useMyAccount } from "../hooks/user";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushUrl } from "../util/history";
import { AddToSuperfile } from "./forms/AddToSuperfile";
import { CopyFile } from "./forms/CopyFile";
import { DesprayFile } from "./forms/DesprayFile";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";
import { RemoteCopy } from "./forms/RemoteCopy";
import { RenameFile } from "./forms/RenameFile";
import { ShortVerticalDivider } from "./Common";

const FilterFields: Fields = {
    "ScopeName": { type: "string", label: nlsHPCC.Scope, readonly: true },
    "LogicalName": { type: "string", label: nlsHPCC.Name, placeholder: nlsHPCC.somefile },
    "Description": { type: "string", label: nlsHPCC.Description, placeholder: nlsHPCC.SomeDescription },
    "Owner": { type: "string", label: nlsHPCC.Owner, placeholder: nlsHPCC.jsmi },
    "Index": { type: "checkbox", label: nlsHPCC.Index },
    "NodeGroup": { type: "target-group", label: nlsHPCC.Group, placeholder: nlsHPCC.Cluster },
    "FileSizeFrom": { type: "string", label: nlsHPCC.FromSizes, placeholder: "4096" },
    "FileSizeTo": { type: "string", label: nlsHPCC.ToSizes, placeholder: "16777216" },
    "FileType": { type: "file-type", label: nlsHPCC.FileType },
    "FirstN": { type: "string", label: nlsHPCC.FirstN, placeholder: "-1" },
    "StartDate": { type: "datetime", label: nlsHPCC.FromDate },
    "EndDate": { type: "datetime", label: nlsHPCC.ToDate },
};

function formatQuery(_filter, mine, currentUser) {
    const filter = { ..._filter };
    if (filter.Index) {
        filter.ContentType = "key";
        delete filter.Index;
    }
    if (filter.StartDate) {
        filter.StartDate = new Date(filter.StartDate).toISOString();
    }
    if (filter.EndDate) {
        filter.EndDate = new Date(filter.StartDate).toISOString();
    }
    if (mine === true) {
        filter.Owner = currentUser?.username;
    }
    return filter;
}

const dfuService = new DFUService({ baseUrl: "" });

const defaultUIState = {
    hasSelection: false,
};

interface ScopesProps {
    filter?: object;
    scope?: any;
}

const emptyFilter = {};

const mergeFileData = (DFULogicalFiles, files) => {
    const data = [];
    const scopes = DFULogicalFiles?.DFULogicalFile?.filter(file => file.isDirectory && file.Directory !== ".") ?? [];

    scopes.forEach((scope, idx) => {
        scope["__hpcc_id"] = scope.Directory + "_" + idx;
        data.push(scope);
    });
    files.forEach((file, idx) => {
        file["__hpcc_id"] = file.Name + "_" + idx;
        file["Name"] = file.Name.split("::").pop();
        data.push(file);
    });

    return data;
};

export const Scopes: React.FunctionComponent<ScopesProps> = ({
    filter = emptyFilter,
    scope
}) => {

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);
    const [filterFields, setFilterFields] = React.useState<Fields>({});

    const [data, setData] = React.useState<any[]>([]);
    const [scopePath, setScopePath] = React.useState<string[]>([]);

    const [showFilter, setShowFilter] = React.useState(false);
    const [showRemoteCopy, setShowRemoteCopy] = React.useState(false);
    const [showCopy, setShowCopy] = React.useState(false);
    const [showRenameFile, setShowRenameFile] = React.useState(false);
    const [showAddToSuperfile, setShowAddToSuperfile] = React.useState(false);
    const [showDesprayFile, setShowDesprayFile] = React.useState(false);
    const [mine, setMine] = React.useState(false);
    const { currentUser } = useMyAccount();
    const [viewByScope, setViewByScope] = React.useState(true);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [, { currencyCode }] = useBuildInfo();

    React.useEffect(() => {
        if (scope === ".") {
            setScopePath([]);
            dfuService.DFUFileView({ Scope: "" }).then(async ({ DFULogicalFiles }) => {
                const rootFiles = await dfuService.DFUFileView({ Scope: "." });
                const files = rootFiles?.DFULogicalFiles?.DFULogicalFile ?? [];
                setData(mergeFileData(DFULogicalFiles, files));
            });
        } else {
            setScopePath(scope.split("::"));
            dfuService.DFUFileView({ Scope: scope }).then(({ DFULogicalFiles }) => {
                let files = DFULogicalFiles?.DFULogicalFile?.filter(file => !file.isDirectory) ?? [];
                if (mine) {
                    files = files.filter(file => file.Owner === currentUser?.username);
                }
                setData(mergeFileData(DFULogicalFiles, files));
            });
        }
    }, [currentUser, mine, scope]);

    //  Grid ---
    const { Grid, selection, refreshTable, copyButtons } = useFluentGrid({
        data,
        primaryID: "__hpcc_id",
        filename: "logicalfiles",
        columns: {
            col1: {
                width: 27,
                disabled: (item) => item ? item.__hpcc_isDir : true,
                selectorType: "checkbox"
            },
            IsProtected: {
                headerIcon: "LockSolid",
                width: 25,
                sortable: false,
                formatter: (_protected) => {
                    if (_protected === true) {
                        return <Icon iconName="LockSolid" />;
                    }
                    return "";
                }
            },
            IsCompressed: {
                headerIcon: "ZipFolder",
                width: 25,
                sortable: false,
                formatter: (compressed) => {
                    if (compressed === true) {
                        return <Icon iconName="ZipFolder" />;
                    }
                    return "";
                }
            },
            __hpcc_displayName: {
                label: nlsHPCC.LogicalName, width: 600,
                formatter: React.useCallback((_, row) => {
                    let name = row.Name;
                    let url = `#/files/${row.NodeGroup ? row.NodeGroup + "/" : ""}${[].concat(".", scopePath, name).join("::")}`;
                    if (row.isDirectory) {
                        name = row.Directory;
                        let path = [].concat(scopePath, row.Directory).join("::");
                        path = (path.startsWith("::")) ? path.substring(2) : path;
                        url = "#/scopes/" + path;
                        return <div style={{ display: "flex", alignItems: "center" }}>
                            <Icon iconName={"FabricFolder"} style={{ fontSize: "1.5em", marginRight: "8px" }} />
                            <Link data-selection-disabled={true} href={url}>{name}</Link>
                        </div>;
                    }
                    return <Link data-selection-disabled={true} href={url}>{name}</Link>;
                }, [scopePath])
            },
            Owner: { label: nlsHPCC.Owner, width: 75 },
            SuperOwners: { label: nlsHPCC.SuperOwner, width: 150 },
            Description: { label: nlsHPCC.Description, width: 150 },
            NodeGroup: { label: nlsHPCC.Cluster, width: 108 },
            RecordCount: {
                label: nlsHPCC.Records, width: 85,
                formatter: (value, row) => Utility.valueCleanUp(value),
            },
            IntSize: {
                label: nlsHPCC.Size, width: 100,
                formatter: (value, row) => Utility.convertedSize(value),
            },
            Parts: {
                label: nlsHPCC.Parts, width: 60,
                formatter: (value, row) => Utility.valueCleanUp(value),
            },
            Modified: { label: nlsHPCC.ModifiedUTCGMT, width: 162 },
            AtRestCost: {
                label: nlsHPCC.FileCostAtRest, width: 100,
                formatter: (cost, row) => `${formatCost(cost ?? 0)} (${currencyCode || "$"})`,
            },
            AccessCost: {
                label: nlsHPCC.FileAccessCost, width: 100,
                formatter: (cost, row) => `${formatCost(cost ?? 0)} (${currencyCode || "$"})`,
            }
        }
    });

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedFiles,
        items: selection.filter(s => s.isDirectory === false).map(s => s.Name),
        onSubmit: React.useCallback(() => {
            WsDfu.DFUArrayAction(selection, "Delete").then(() => refreshTable(true));
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
                if (selection.length === 1 && selection[0].isDirectory === false) {
                    window.location.href = "#/files/" + (selection[0].NodeGroup ? selection[0].NodeGroup + "/" : "") + selection[0].Name;
                } else {
                    const _files = selection.filter(s => s.isDirectory === false);
                    for (let i = _files.length - 1; i >= 0; --i) {
                        window.open("#/files/" + (_files[i].NodeGroup ? _files[i].NodeGroup + "/" : "") + _files[i].Name, "_blank");
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
            key: "filter", text: nlsHPCC.Filter, disabled: !data, iconProps: { iconName: hasFilter ? "FilterSolid" : "Filter" },
            onClick: () => setShowFilter(true)
        },
        {
            key: "viewByScope", text: nlsHPCC.ViewByScope, iconProps: { iconName: "BulletedTreeList" }, iconOnly: true, canCheck: true, checked: viewByScope,
            onClick: () => {
                setViewByScope(!viewByScope);
                window.location.href = "#/files";
            }
        },
        { key: "divider_5", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "mine", text: nlsHPCC.Mine, disabled: !currentUser, iconProps: { iconName: "Contact" }, canCheck: true, checked: mine,
            onClick: () => setMine(!mine)
        },
    ], [currentUser, data, hasFilter, mine, refreshTable, selection, setShowDeleteConfirm, uiState.hasSelection, viewByScope]);

    //  Filter  ---
    React.useEffect(() => {
        const _filterFields: Fields = {};
        for (const field in FilterFields) {
            _filterFields[field] = { ...FilterFields[field], value: filter[field] };
        }
        _filterFields["ScopeName"].value = scope;
        setFilterFields(_filterFields);
    }, [filter, scope]);

    const applyFilter = React.useCallback((params) => {
        const query = formatQuery(params, mine, currentUser);

        if (query["ScopeName"] !== ".") {
            query["LogicalName"] = query["ScopeName"] + (query["LogicalName"] ? "::" + query["LogicalName"] : "");
        }
        delete query["ScopeName"];

        const keys = Object.keys(query);
        const qs = keys.map(key => {
            const val = query[key];
            if (!!val) {
                return `${key}=${val}`;
            }
            return "";
        }).filter(pair => pair.length > 0).join("&");

        pushUrl("/files/?" + qs);
    }, [currentUser, mine]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };
        const _selection = selection.filter(s => s.isDirectory === false);

        for (let i = 0; i < _selection.length; ++i) {
            state.hasSelection = true;
            //  TODO:  More State
        }
        setUIState(state);
    }, [selection]);

    return <HolyGrail
        header={
            <>
                <CommandBar items={buttons} farItems={copyButtons} />
                <ScopesBreadcrumb scope={scope} />
            </>
        }
        main={
            <>
                <SizeMe monitorHeight>{({ size }) =>
                    <div style={{ position: "relative", width: "100%", height: "100%" }}>
                        <div style={{ position: "absolute", width: "100%", height: `${size.height}px` }}>
                            <Grid height={`${size.height}px`} />
                        </div>
                    </div>
                }</SizeMe>
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={applyFilter} />
                <RemoteCopy showForm={showRemoteCopy} setShowForm={setShowRemoteCopy} refreshGrid={refreshTable} />
                <CopyFile logicalFiles={selection.filter(s => s.isDirectory === false).map(s => s.Name)} showForm={showCopy} setShowForm={setShowCopy} refreshGrid={refreshTable} />
                <RenameFile logicalFiles={selection.filter(s => s.isDirectory === false).map(s => s.Name)} showForm={showRenameFile} setShowForm={setShowRenameFile} refreshGrid={refreshTable} />
                <AddToSuperfile logicalFiles={selection.filter(s => s.isDirectory === false).map(s => s.Name)} showForm={showAddToSuperfile} setShowForm={setShowAddToSuperfile} refreshGrid={refreshTable} />
                <DesprayFile logicalFiles={selection.filter(s => s.isDirectory === false).map(s => s.Name)} showForm={showDesprayFile} setShowForm={setShowDesprayFile} />
                <DeleteConfirm />
            </>
        }
    />;
};

interface ScopesBreadcrumbProps {
    scope: string;
}

export const ScopesBreadcrumb: React.FunctionComponent<ScopesBreadcrumbProps> = ({
    scope
}) => {

    const [theme] = useUserTheme();
    const breadcrumbStyles = React.useMemo(() => mergeStyleSets({
        wrapper: {
            padding: "1em 2.5em",
            borderTop: `1px solid ${theme.palette.neutralLight}`,
            borderBottom: `1px solid ${theme.palette.neutralLight}`,
        },
        separator: {
            margin: "0 0.5em"
        }
    }), [theme]);

    const [scopePath, setScopePath] = React.useState([]);

    React.useEffect(() => {
        setScopePath(scope.split("::"));
    }, [scope]);

    return <div className={breadcrumbStyles.wrapper}>
        <Link href="#/scopes">Root Scope</Link>
        {(scope !== ".") &&
            <>
                {scopePath.map((scope, idx) => {
                    if (idx === scopePath.length - 1) {
                        return <span key={`${scope}_${idx}`}>
                            <span className={breadcrumbStyles.separator}>::</span>
                            <span>{scope}</span>
                        </span>;
                    }
                    const path = scopePath.slice(0, idx + 1).join("::");
                    return <span key={`${scope}_${idx}`}>
                        <span className={breadcrumbStyles.separator}>::</span>
                        <Link href={`#/scopes/${path}`}>{scope}</Link>
                    </span>;

                })}
            </>
        }
    </div>;

};