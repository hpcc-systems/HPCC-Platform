import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link } from "@fluentui/react";
import { FolderZip20Regular, LockClosedFilled } from "@fluentui/react-icons";
import { TableCellLayout, TableColumnDefinition, TableRowId, createTableColumn, Tooltip } from "@fluentui/react-components";
import { WsDfu as HPCCWsDfu } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { SizeMe } from "react-sizeme";
import * as WsDfu from "src/WsDfu";
import { CreateDFUQueryStore } from "src/ESPLogicalFile";
import { formatCost } from "src/Session";
import * as Utility from "src/Utility";
import { QuerySortItem } from "src/store/Store";
import nlsHPCC from "src/nlsHPCC";
import { useConfirm } from "../hooks/confirm";
import { useUserTheme } from "../hooks/theme";
import { useMyAccount } from "../hooks/user";
import { useHasFocus, useIsMounted } from "../hooks/util";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { FluentPagedDataGrid, FluentPagedFooter, useCopyButtons, useFluentStoreState } from "./controls/Grid";
import { AddToSuperfile } from "./forms/AddToSuperfile";
import { CopyFile } from "./forms/CopyFile";
import { DesprayFile } from "./forms/DesprayFile";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";
import { RemoteCopy } from "./forms/RemoteCopy";
import { RenameFile } from "./forms/RenameFile";
import { ShortVerticalDivider } from "./Common";

const logger = scopedLogger("src-react/components/Files.tsx");

type LogicalFile = HPCCWsDfu.DFULogicalFile;

const FilterFields: Fields = {
    "LogicalName": { type: "string", label: nlsHPCC.Name, placeholder: nlsHPCC.somefile },
    "Description": { type: "string", label: nlsHPCC.Description, placeholder: nlsHPCC.SomeDescription },
    "Owner": { type: "string", label: nlsHPCC.Owner, placeholder: nlsHPCC.jsmi },
    "LogicalFiles": { type: "checkbox", label: nlsHPCC.LogicalFiles },
    "SuperFiles": { type: "checkbox", label: nlsHPCC.SuperFiles },
    "Indexes": { type: "checkbox", label: nlsHPCC.Indexes },
    "NotInSuperfiles": { type: "checkbox", label: nlsHPCC.NotInSuperfiles, disabled: (params: Fields) => !!params?.SuperFiles?.value || !!params?.LogicalFiles?.value },
    "NodeGroup": { type: "target-group", label: nlsHPCC.Cluster, placeholder: nlsHPCC.Cluster, multiSelect: true, valueSeparator: "," },
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

    const { themeV9 } = useUserTheme();
    const footerStyles = React.useMemo(() => {
        return {
            zIndex: 2,
            background: themeV9.colorNeutralBackground1,
            borderTop: `1px solid ${themeV9.colorNeutralStroke1}`
        };
    }, [themeV9]);

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
    const {
        selection, setSelection,
        pageNum, setPageNum,
        pageSize, setPageSize,
        total, setTotal,
        refreshTable } = useFluentStoreState({ page });

    const [selectedRows, setSelectedRows] = React.useState(new Set<TableRowId>());
    const onSelectionChange = (items, rowIds) => {
        setSelectedRows(rowIds);
        setSelection(items);
    };

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
    const gridStore = React.useMemo(() => {
        return store ? store : CreateDFUQueryStore();
    }, [store]);

    const query = React.useMemo(() => {
        return formatQuery(filter);
    }, [filter]);

    const columnSizingOptions = React.useMemo(() => {
        return {
            IsProtected: { minWidth: 16, defaultWidth: 16 },
            IsCompressed: { minWidth: 16, defaultWidth: 16 },
            Name: { minWidth: 160, idealWidth: 360, defaultWidth: 360 },
            Owner: { minWidth: 90, idealWidth: 120, defaultWidth: 120 },
            SuperOwners: { minWidth: 90, idealWidth: 90, defaultWidth: 90 },
            Description: { minWidth: 60, idealWidth: 80, defaultWidth: 80 },
            Cluster: { minWidth: 64, defaultWidth: 64 },
            Records: { minWidth: 64, defaultWidth: 64 },
            Size: { minWidth: 32, defaultWidth: 64 },
            Parts: { minWidth: 32, defaultWidth: 64 },
            MinSkew: { minWidth: 64, defaultWidth: 64 },
            MaxSkew: { minWidth: 64, defaultWidth: 64 },
            Modified: { minWidth: 160, idealWidth: 160, defaultWidth: 160 },
            Accessed: { minWidth: 130, idealWidth: 130, defaultWidth: 130 },
        };
    }, []);

    const columns: TableColumnDefinition<LogicalFile>[] = React.useMemo(() => [
        createTableColumn<LogicalFile>({
            columnId: "IsProtected",
            renderHeaderCell: () => <Tooltip content={nlsHPCC.Protected} relationship="label"><LockClosedFilled fontSize={18} /></Tooltip>,
            renderCell: (cell) => cell.IsProtected ? <LockClosedFilled /> : "",
        }),
        createTableColumn<LogicalFile>({
            columnId: "IsCompressed",
            renderHeaderCell: () => <Tooltip content={nlsHPCC.Compressed} relationship="label"><FolderZip20Regular /></Tooltip>,
            renderCell: (cell) => cell.IsCompressed ? <FolderZip20Regular /> : "",
        }),
        createTableColumn<LogicalFile>({
            columnId: "Name",
            compare: (a, b) => a.Name?.localeCompare(b.Name),
            renderHeaderCell: () => nlsHPCC.LogicalName,
            renderCell: (cell) => {
                return <TableCellLayout>
                    <Link href={`#/files/${cell.Name}`}>{cell.Name}</Link>
                </TableCellLayout>;
            },
        }),
        createTableColumn<LogicalFile>({
            columnId: "Owner",
            compare: (a, b) => a.Owner?.localeCompare(b.Owner),
            renderHeaderCell: () => nlsHPCC.Owner,
            renderCell: (cell) => <TableCellLayout>{cell?.Owner}</TableCellLayout>,
        }),
        createTableColumn<LogicalFile>({
            columnId: "SuperOwners",
            compare: (a, b) => a.SuperOwners?.localeCompare(b.SuperOwners),
            renderHeaderCell: () => nlsHPCC.SuperOwner,
            renderCell: (cell) => <TableCellLayout>{cell?.SuperOwners}</TableCellLayout>,
        }),
        createTableColumn<LogicalFile>({
            columnId: "Description",
            compare: (a, b) => a.Description?.localeCompare(b.Description),
            renderHeaderCell: () => nlsHPCC.Description,
            renderCell: (cell) => <TableCellLayout>{cell?.Description}</TableCellLayout>,
        }),
        createTableColumn<LogicalFile>({
            columnId: "Cluster",
            compare: (a, b) => a["Cluster"]?.localeCompare(b["Cluster"]),
            renderHeaderCell: () => nlsHPCC.Cluster,
            renderCell: (cell) => <TableCellLayout>{cell["Cluster"]}</TableCellLayout>,
        }),
        createTableColumn<LogicalFile>({
            columnId: "Records",
            compare: (a, b) => a["Records"]?.localeCompare(b["Records"]),
            renderHeaderCell: () => nlsHPCC.Records,
            renderCell: (cell) => <TableCellLayout>{cell["Records"]}</TableCellLayout>,
        }),
        createTableColumn<LogicalFile>({
            columnId: "Size",
            compare: (a, b) => a["Size"]?.localeCompare(b["Size"]),
            renderHeaderCell: () => nlsHPCC.Size,
            renderCell: (cell) => <TableCellLayout>{cell["Size"]}</TableCellLayout>,
        }),
        createTableColumn<LogicalFile>({
            columnId: "Parts",
            compare: (a, b) => a["Parts"]?.localeCompare(b["Parts"]),
            renderHeaderCell: () => nlsHPCC.Parts,
            renderCell: (cell) => <TableCellLayout>{cell["Parts"]}</TableCellLayout>,
        }),
        createTableColumn<LogicalFile>({
            columnId: "MinSkew",
            compare: (a, b) => a["MinSkew"] - b["MinSkew"],
            renderHeaderCell: () => nlsHPCC.MinSkew,
            renderCell: (cell) => <TableCellLayout>{cell.MinSkew ? `${Utility.formatDecimal(cell.MinSkew / 100)}%` : ""}</TableCellLayout>,
        }),
        createTableColumn<LogicalFile>({
            columnId: "MaxSkew",
            compare: (a, b) => a["MaxSkew"] - b["MaxSkew"],
            renderHeaderCell: () => nlsHPCC.MaxSkew,
            renderCell: (cell) => <TableCellLayout>{cell.MaxSkew ? `${Utility.formatDecimal(cell.MaxSkew / 100)}%` : ""}</TableCellLayout>,
        }),
        createTableColumn<LogicalFile>({
            columnId: "Modified",
            compare: (a, b) => a.Modified?.localeCompare(b.Modified),
            renderHeaderCell: () => nlsHPCC.ModifiedUTCGMT,
            renderCell: (cell) => <TableCellLayout>{cell?.Modified}</TableCellLayout>,
        }),
        createTableColumn<LogicalFile>({
            columnId: "Accessed",
            compare: (a, b) => a.Accessed?.localeCompare(b.Accessed),
            renderHeaderCell: () => nlsHPCC.LastAccessed,
            renderCell: (cell) => <TableCellLayout>{cell?.Accessed}</TableCellLayout>,
        }),
        createTableColumn<LogicalFile>({
            columnId: "AtRestCost",
            compare: (a, b) => a.AtRestCost - b.AtRestCost,
            renderHeaderCell: () => nlsHPCC.FileCostAtRest,
            renderCell: (cell) => <TableCellLayout>{formatCost(cell.AtRestCost)}</TableCellLayout>,
        }),
        createTableColumn<LogicalFile>({
            columnId: "AccessCost",
            compare: (a, b) => a.AccessCost - b.AccessCost,
            renderHeaderCell: () => nlsHPCC.FileAccessCost,
            renderCell: (cell) => <TableCellLayout>{formatCost(cell.AccessCost)}</TableCellLayout>,
        }),
    ], []);

    const columnMap: Utility.ColumnMap = React.useMemo(() => {
        const retVal: Utility.ColumnMap = {};
        columns.forEach((col, idx) => {
            const columnId = col.columnId.toString();
            retVal[columnId] = {
                id: `${columnId}_${idx}`,
                field: columnId,
                label: columnId
            };
        });
        return retVal;
    }, [columns]);

    const copyButtons = useCopyButtons(columnMap, selection, "files");

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedFiles,
        items: selection.map(s => s.Name),
        onSubmit: React.useCallback(() => {
            WsDfu.DFUArrayAction(selection, "Delete")
                .then(({ DFUArrayActionResponse }) => {
                    const ActionResults = DFUArrayActionResponse?.ActionResults?.DFUActionInfo ?? [];
                    ActionResults.filter(action => action?.Failed).forEach(action => logger.error(action?.ActionResult));
                    refreshTable.call(true);
                })
                .catch(err => logger.error(err));
        }, [refreshTable, selection])
    });

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
    ], [currentUser, filter, hasFilter, refreshTable, selection, setShowDeleteConfirm, store, total, uiState.hasSelection, viewByScope]);

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
                            <FluentPagedDataGrid
                                store={gridStore}
                                query={query}
                                sort={sort}
                                pageNum={pageNum}
                                pageSize={pageSize}
                                total={total}
                                columns={columns}
                                sizingOptions={columnSizingOptions}
                                height={"calc(100vh - 176px)"}
                                onSelect={onSelectionChange}
                                selectedItems={selectedRows}
                                setSelection={setSelection}
                                setTotal={setTotal}
                                refresh={refreshTable}
                            ></FluentPagedDataGrid>
                        </div>
                    </div>
                }</SizeMe>
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
                <RemoteCopy showForm={showRemoteCopy} setShowForm={setShowRemoteCopy} refreshGrid={refreshTable.call} />
                <CopyFile logicalFiles={selection.map(s => s.Name)} showForm={showCopy} setShowForm={setShowCopy} refreshGrid={refreshTable.call} />
                <RenameFile logicalFiles={selection.map(s => s.Name)} showForm={showRenameFile} setShowForm={setShowRenameFile} refreshGrid={refreshTable.call} />
                <AddToSuperfile logicalFiles={selection.map(s => s.Name)} showForm={showAddToSuperfile} setShowForm={setShowAddToSuperfile} refreshGrid={refreshTable.call} />
                <DesprayFile logicalFiles={selection.map(s => s.Name)} showForm={showDesprayFile} setShowForm={setShowDesprayFile} />
                <DeleteConfirm />
            </>
        }
        footer={<FluentPagedFooter
            persistID={"files"}
            pageNum={pageNum}
            selectionCount={selection.length}
            setPageNum={setPageNum}
            setPageSize={setPageSize}
            total={total}
        ></FluentPagedFooter>}
        footerStyles={footerStyles}
    />;
};
