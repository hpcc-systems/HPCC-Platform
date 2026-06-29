import * as React from "react";
import { Button, DataGrid, DataGridHeader, DataGridHeaderCell, DataGridBody, DataGridRow, DataGridCell, createTableColumn, TableColumnDefinition, TableColumnSizingOptions, TableRowId, TableRow, TableCell, TableSelectionCell, Tooltip, Dropdown, Option, SkeletonItem, makeStyles, tokens } from "@fluentui/react-components";
import { ChevronDoubleLeft20Regular, ChevronLeft20Regular, ChevronRight20Regular, ChevronDoubleRight20Regular } from "@fluentui/react-icons";
import { useConst } from "@fluentui/react-hooks";
import { BaseStore, Memory, QueryRequest, QuerySortItem } from "src/store/Memory";
import nlsHPCC from "src/nlsHPCC";
import { updatePage, updateSort } from "../../util/history";
import { useDeepCallback, useDeepEffect, useDeepMemo } from "../../hooks/deepHooks";
import { useUserStore, useNonReactiveEphemeralPageStore } from "../../hooks/store";
import { ICommandBarItemProps } from "../CommandBarV9";
import { createCopyDownloadSelection } from "../Common";

// ─── Local replacements for @fluentui/react exports ─────────────────────────

export const SelectionMode = { none: 0, single: 1, multiple: 2 } as const;
export type SelectionMode = typeof SelectionMode[keyof typeof SelectionMode];

export interface ISelectionOptions {
    getKey?: (item: any) => string;
    canSelectItem?: (item: any, index?: number) => boolean;
    onSelectionChanged?: () => void;
    onItemsChanged?: () => void;
}

export interface ISelection {
    getSelection(): any[];
    getSelectedIndices(): number[];
    setItems(items: any[], shouldClearSelection?: boolean): void;
    setIndexSelected(index: number, isSelected: boolean, shouldAnchor: boolean): void;
    setAllSelected(isAllSelected: boolean): void;
    setKeySelected(key: string, isSelected: boolean, shouldAnchor: boolean): void;
    setChangeEvents(isEnabled: boolean, suppressEvents?: boolean): void;
    getItems(): any[];
}

export class Selection implements ISelection {
    private _items: any[] = [];
    private _selectedKeys: Set<string> = new Set();
    private _changeEventsEnabled = true;
    private _pendingChange = false;
    private readonly _getKey: (item: any) => string;
    private readonly _canSelectItem?: (item: any, index?: number) => boolean;
    private readonly _onSelectionChanged?: () => void;
    private readonly _onItemsChanged?: () => void;
    private _changeListeners: Array<() => void> = [];

    constructor(options?: ISelectionOptions) {
        this._getKey = options?.getKey ?? ((item: any) => item?.key ?? String(item));
        this._canSelectItem = options?.canSelectItem;
        this._onSelectionChanged = options?.onSelectionChanged;
        this._onItemsChanged = options?.onItemsChanged;
    }

    addChangeListener(listener: () => void): void { this._changeListeners.push(listener); }
    removeChangeListener(listener: () => void): void { this._changeListeners = this._changeListeners.filter(l => l !== listener); }

    private _fireChange(): void {
        if (this._changeEventsEnabled) {
            this._onSelectionChanged?.();
            this._changeListeners.forEach(l => l());
        } else {
            this._pendingChange = true;
        }
    }

    getItems(): any[] { return this._items; }
    getSelection(): any[] { return this._items.filter(item => this._selectedKeys.has(this._getKey(item))); }
    getSelectedIndices(): number[] {
        return this._items.reduce((acc: number[], item, index) => {
            if (this._selectedKeys.has(this._getKey(item))) acc.push(index);
            return acc;
        }, []);
    }

    setItems(items: any[], shouldClearSelection = false): void {
        this._items = items;
        if (shouldClearSelection) { this._selectedKeys.clear(); }
        this._onItemsChanged?.();
    }

    setIndexSelected(index: number, isSelected: boolean, _shouldAnchor: boolean): void {
        const item = this._items[index];
        if (!item) return;
        if (this._canSelectItem && !this._canSelectItem(item, index)) return;
        const key = this._getKey(item);
        if (isSelected) { this._selectedKeys.add(key); } else { this._selectedKeys.delete(key); }
        this._fireChange();
    }

    setAllSelected(isAllSelected: boolean): void {
        if (isAllSelected) {
            this._items.forEach((item, index) => {
                if (!this._canSelectItem || this._canSelectItem(item, index)) {
                    this._selectedKeys.add(this._getKey(item));
                }
            });
        } else { this._selectedKeys.clear(); }
        this._fireChange();
    }

    setKeySelected(key: string, isSelected: boolean, _shouldAnchor: boolean): void {
        if (isSelected) { this._selectedKeys.add(key); } else { this._selectedKeys.delete(key); }
        this._fireChange();
    }

    setChangeEvents(isEnabled: boolean, suppressEvents = false): void {
        this._changeEventsEnabled = isEnabled;
        if (isEnabled && this._pendingChange && !suppressEvents) {
            this._pendingChange = false;
            this._onSelectionChanged?.();
            this._changeListeners.forEach(l => l());
        }
    }
}

// Internal column type (replaces v8 _IColumn)
interface IColumn {
    key: string;
    name: string;
    fieldName?: string;
    minWidth?: number;
    maxWidth?: number;
    isResizable?: boolean;
    isSorted?: boolean;
    isSortedDescending?: boolean;
    iconName?: string;
    isIconOnly?: boolean;
    flexGrow?: number;
    data: FluentColumn;
    onRender?: (item: any, index: number, column: IColumn) => React.ReactNode;
}

// Row props — exported for consumers (Workunits.tsx)
export interface IDetailsRowProps {
    item: any;
    itemIndex: number;
    overlayElement?: React.ReactNode;
    _internal?: {
        columns: IColumn[];
        selectionMode: SelectionMode;
        isSelected: boolean;
        onToggle: () => void;
    };
}

const useGridStyles = makeStyles({
    cell: {
        borderRight: "1px solid var(--colorNeutralBackground5)",
        overflow: "hidden",
        "&:has(.bgFilled)": { color: "white", boxShadow: "inset 1px 0 var(--colorNeutralBackground1), inset -1px 1px var(--colorNeutralBackground1)" },
        "&:has(.bgGreen)": { background: "green" },
        "&:has(.bgOrange)": { background: "orange" },
        "&:has(.bgRed)": { background: "red" },
    },
    selectionCell: {
        borderRight: "1px solid var(--colorNeutralBackground5)",
    },
    header: {
        position: "sticky",
        top: 0,
        zIndex: 2,
        backgroundColor: tokens.colorNeutralBackground1,
    },
    headerCell: {
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
        borderRight: "1px solid var(--colorNeutralBackground5)",
    },
});

export const DetailsRow: React.FunctionComponent<IDetailsRowProps> = ({ item, itemIndex, _internal, overlayElement }) => {
    const styles = useGridStyles();
    if (!_internal) return null;
    const { columns, selectionMode, isSelected, onToggle } = _internal;
    return <TableRow
        aria-selected={isSelected}
        onClick={selectionMode !== SelectionMode.none ? onToggle : undefined}
        style={{ position: "relative", cursor: selectionMode !== SelectionMode.none ? "pointer" : "default" }}
    >
        {selectionMode === SelectionMode.multiple && (
            <TableSelectionCell type="checkbox" checked={isSelected} style={{ width: 32, minWidth: 32, maxWidth: 32, borderRight: "1px solid var(--colorNeutralBackground5)" }} onClick={e => { e.stopPropagation(); onToggle(); }} />
        )}
        {selectionMode === SelectionMode.single && (
            <TableSelectionCell type="radio" checked={isSelected} style={{ width: 32, minWidth: 32, maxWidth: 32, borderRight: "1px solid var(--colorNeutralBackground5)" }} onClick={e => { e.stopPropagation(); onToggle(); }} />
        )}
        {columns.map(col => (
            <TableCell key={col.key} className={styles.cell} style={{ width: col.minWidth ?? 70, minWidth: col.minWidth ?? 70, maxWidth: col.maxWidth, flexGrow: col.flexGrow ?? 0 }}>
                {col.onRender ? col.onRender(item, itemIndex, col) : <span style={{ display: "block", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{String(item[col.fieldName ?? col.key] ?? "")}</span>}
            </TableCell>
        ))}
        {overlayElement && (
            <td aria-hidden style={{ position: "absolute", inset: 0, padding: 0, border: "none", pointerEvents: "none", overflow: "hidden" }}>
                {overlayElement}
            </td>
        )}
    </TableRow>;
};

// ─────────────────────────────────────────────────────────────────────────────

/*  ---  Debugging dependency changes  ---
 *
 *  import { useWhatChanged } from "@simbathesailor/use-what-changed";
 *
 *  useWhatChanged([count, selectionHandler, sorted, start, store, query], "count, selectionHandler, sorted, start, store, query");
 *
 */

type SelectorType = "checkbox";
type JustifyType = "left" | "right";
export interface FluentColumn {
    selectorType?: SelectorType;
    label?: string;
    field?: string;
    width?: number;
    minWidth?: number;
    maxWidth?: number;
    resizable?: boolean;
    headerIconElement?: React.ReactNode;
    headerTooltip?: string;
    sortable?: boolean;
    disabled?: boolean | ((item: any) => boolean);
    hidden?: boolean;
    justify?: JustifyType;
    formatter?: (value: any, row: any) => any;
    csvFormatter?: (value: any, row: any) => string;
    className?: (value: any, row: any) => string;
}

export type FluentColumns = { [key: string]: FluentColumn };

function isISelection(selection: any): selection is ISelection {
    return typeof selection === "object" && selection !== null && "setAllSelected" in selection;
}

function tooltipItemRenderer(item: any, index: number, column: IColumn) {
    const value = item[column.fieldName || column.key];
    const className = column.data.className ? column.data.className(value, item) : "";
    const style: React.CSSProperties = {
        display: "block",
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
        textAlign: column.data.justify === "right" ? "right" : "left"
    };
    const formattedValue = column.data.formatter
        ? column.data.formatter(value, item) ?? ""
        : value ?? "";
    const tooltipValue = (typeof formattedValue === "string" || typeof formattedValue === "number") ? formattedValue : value ?? "";
    const content = <span style={style} className={className}>{formattedValue}</span>;
    if (tooltipValue === "" || tooltipValue === undefined || tooltipValue === null) {
        return <span style={{ flex: 1, minWidth: 0, overflow: "hidden" }}>{content}</span>;
    }
    return <Tooltip content={String(tooltipValue)} relationship="description" withArrow>
        <span style={{ flex: 1, minWidth: 0, overflow: "hidden" }}>{content}</span>
    </Tooltip>;
}

function columnsAdapter(columns: FluentColumns, columnWidths: Map<string, any>): IColumn[] {
    const retVal: IColumn[] = [];
    for (const key in columns) {
        const column = columns[key];
        const persistedWidth = columnWidths.get(key);
        const width = persistedWidth ?? column.width;
        if (column?.selectorType === undefined && column?.hidden !== true) {
            const bothFixed = column.minWidth !== undefined && column.maxWidth !== undefined;
            retVal.push({
                key,
                name: column.label ?? key,
                fieldName: column.field ?? key,
                minWidth: width ?? column.minWidth ?? 70,
                maxWidth: persistedWidth ?? column.maxWidth,
                isResizable: column.resizable !== undefined ? column.resizable : !bothFixed,
                isSorted: false,
                isSortedDescending: false,
                isIconOnly: !!column.headerIconElement,
                data: column,
                onRender: (item: any, index: number, col: IColumn) => {
                    return tooltipItemRenderer(item, index, col);
                }
            });
        }
    }
    return retVal;
}

export function useCopyButtons(columns: FluentColumns, selection: any[], filename: string): ICommandBarItemProps[] {

    const memoizedColumns = useDeepMemo(() => columns, [], [columns]);

    const copyButtons = React.useMemo((): ICommandBarItemProps[] => [
        ...createCopyDownloadSelection(memoizedColumns, selection, `${filename}.csv`)
    ], [memoizedColumns, filename, selection]);

    return copyButtons;
}

interface RefreshTable {
    call: (clearSelection?: boolean) => void;
    value: number;
    clear: boolean;
}

function useRefreshTable(): RefreshTable {

    const [value, setValue] = React.useState(0);
    const [clear, setClear] = React.useState(false);
    const call = React.useCallback((clearSelection?: boolean) => {
        setValue(value + 1);
        setClear(clearSelection);
    }, [value]);
    return { call, value, clear };
}

export interface FluentStoreStateProps {
    page?: number
}

export interface FluentStoreStateResponse {
    selection: any[];
    setSelection: (selection: any[]) => void;
    pageNum: number;
    setPageNum: (pageNum: number) => void;
    pageSize: number;
    setPageSize: (pageSize: number) => void;
    total: number;
    setTotal: (total: number) => void;
    refreshTable: RefreshTable;
}

export function useFluentStoreState({ page }: FluentStoreStateProps): FluentStoreStateResponse {
    const [selection, setSelection] = React.useState([]);
    const [pageNum, setPageNum] = React.useState(page);
    const [pageSize, setPageSize] = React.useState<number>();
    const [total, setTotal] = React.useState(-1);
    const refreshTable = useRefreshTable();

    return { selection, setSelection, pageNum, setPageNum, pageSize, setPageSize, total, setTotal, refreshTable };
}

interface FluentStoreGridProps {
    store: any,
    query?: QueryRequest,
    sort?: QuerySortItem,
    start: number,
    count: number,
    columns: FluentColumns,
    height: string,
    refresh: RefreshTable,
    selectionMode?: SelectionMode,
    setSelection: ISelection | ((selection: any[]) => void),
    setTotal: (total: number) => void,
    onRenderRow?: (props: IDetailsRowProps) => React.ReactNode,
    canSelectRow?: (item: any, index: number) => boolean
}

const FluentStoreGrid: React.FunctionComponent<FluentStoreGridProps> = ({
    store,
    query,
    sort,
    start,
    count,
    columns,
    height,
    refresh,
    selectionMode = SelectionMode.multiple,
    setSelection,
    setTotal,
    onRenderRow,
    canSelectRow
}) => {
    const memoizedColumns = useDeepMemo(() => columns, [], [columns]);
    const [sorted, setSorted] = React.useState<QuerySortItem>(sort);
    const [items, setItems] = React.useState<any[]>([]);
    const [loaded, setLoaded] = React.useState(false);
    const [columnWidths] = useNonReactiveEphemeralPageStore("columnWidths");
    const [selectedRowIds, setSelectedRowIds] = React.useState<Set<TableRowId>>(new Set());

    const getRowId = React.useCallback((item: any) => "__shimmerIndex__" in item ? `__shimmer_${item.__shimmerIndex__}` : String(store.getIdentity(item)), [store]);

    //  Selection handler - supports an external ISelection (e.g. MetricsOptions) or an internal one bridged to a callback
    const canSelectRowRef = React.useRef(canSelectRow);
    canSelectRowRef.current = canSelectRow;

    const handlerRef = React.useRef<ISelection>(null);
    const selectionHandler = useConst(() => {
        handlerRef.current = isISelection(setSelection) ? setSelection : new Selection({
            getKey: (item: any) => String(store.getIdentity(item)),
            canSelectItem: (item: any, index: number) => !canSelectRowRef.current || canSelectRowRef.current(item, index),
            onSelectionChanged: () => {
                (setSelection as (s: any[]) => void)(handlerRef.current!.getSelection());
            }
        });
        return handlerRef.current;
    });

    const syncSelectedRowIds = React.useCallback(() => {
        setSelectedRowIds(new Set(selectionHandler.getSelection().map(getRowId)));
    }, [selectionHandler, getRowId]);

    //  Reflect external ISelection changes (MetricsOptions ISelection form)
    React.useEffect(() => {
        if (setSelection instanceof Selection) {
            setSelection.addChangeListener(syncSelectedRowIds);
            return () => setSelection.removeChangeListener(syncSelectedRowIds);
        }
    }, [setSelection, syncSelectedRowIds]);

    const abortController = React.useRef<AbortController | undefined>(undefined);

    useDeepEffect(() => {
        if (abortController.current) {
            abortController.current.abort({ message: nlsHPCC.GridAbortMessage });
        }
        abortController.current = new AbortController();
    }, [], [query, sorted]);

    const refreshTable = useDeepCallback((clearSelection = false) => {
        if (isNaN(start) || isNaN(count)) return;
        setLoaded(false);
        if (clearSelection) {
            selectionHandler.setItems([], true);
        }
        const storeQuery = store.query({ ...query }, { start, count, sort: sorted ? [sorted] : undefined }, abortController.current.signal);
        storeQuery.total.then(total => {
            setTotal(total);
        });
        storeQuery.then(items => {
            setLoaded(true);
            setItems(items);
            selectionHandler.setItems(items, false);
            syncSelectedRowIds();
            //  Sync selection state for callback-based setSelection to avoid stale parent selection
            if (!isISelection(setSelection)) {
                (setSelection as (s: any[]) => void)(selectionHandler.getSelection());
            }
        });
    }, [count, selectionHandler, start, store, syncSelectedRowIds], [query, sorted]);

    React.useEffect(() => {
        // eslint-disable-next-line @typescript-eslint/no-unused-expressions
        refresh.value; // Dummy line to ensure its included in the dependency array
        refreshTable(refresh.clear);
    }, [refresh.clear, refresh.value, refreshTable]);

    useDeepEffect(() => {
        setSorted(sort);
    }, [], [sort]);

    const fluentColumns: IColumn[] = React.useMemo(() => {
        return columnsAdapter(memoizedColumns, columnWidths as Map<string, number>);
    }, [columnWidths, memoizedColumns]);

    const tableColumns = React.useMemo<TableColumnDefinition<any>[]>(() => fluentColumns.map(col =>
        createTableColumn<any>({
            columnId: col.key,
            //  DataGrid treats a column as sortable when its compare fn declares > 0 params; sorting itself is performed server-side by the store
            compare: col.data?.sortable === false ? () => 0 : (_a: any, _b: any) => 0,
            renderHeaderCell: () => col.isIconOnly
                ? <Tooltip content={col.data?.headerTooltip ?? col.name} relationship="label"><span aria-label={col.name} style={{ display: "flex", alignItems: "center" }}>{col.data?.headerIconElement ?? <span>&#x2002;</span>}</span></Tooltip>
                : <>{col.name}</>,
            renderCell: (item: any) => col.onRender ? col.onRender(item, -1, col) : <>{String(item[col.fieldName ?? col.key] ?? "")}</>
        })
    ), [fluentColumns]);

    const columnSizingOptions = React.useMemo<TableColumnSizingOptions>(() => {
        const widths = columnWidths as Map<string, number>;
        const opts: TableColumnSizingOptions = {};
        for (const key in memoizedColumns) {
            const col = memoizedColumns[key];
            if (col.selectorType !== undefined || col.hidden === true) continue;
            opts[key] = {
                minWidth: col.minWidth ?? col.width ?? 40,
                idealWidth: widths.get(key) ?? col.width ?? col.minWidth ?? 70
            };
        }
        return opts;
    }, [memoizedColumns, columnWidths]);

    const sortState = React.useMemo(() => ({
        sortColumn: (sorted?.attribute as string) || undefined,
        sortDirection: sorted === undefined ? undefined : (sorted?.descending ? "descending" : "ascending") as "ascending" | "descending"
    }), [sorted]);

    const onSortChange = React.useCallback((_ev: React.MouseEvent, next: { sortColumn: TableRowId | undefined; sortDirection: "ascending" | "descending"; }) => {
        if (next.sortColumn === undefined) return;
        const colKey = String(next.sortColumn);
        if (memoizedColumns[colKey]?.sortable === false) return;
        const descending = next.sortDirection === "descending";
        setSorted({ attribute: colKey, descending });
        updateSort(true, descending, colKey);
    }, [memoizedColumns]);

    const onSelectionChange = React.useCallback((_ev: React.SyntheticEvent, data: { selectedItems: Set<TableRowId>; }) => {
        const ids = data.selectedItems;
        const allowedIds = new Set<TableRowId>();
        selectionHandler.setChangeEvents(false, true);
        selectionHandler.setAllSelected(false);
        items.forEach((item, index) => {
            if (ids.has(getRowId(item)) && (!canSelectRow || canSelectRow(item, index))) {
                selectionHandler.setIndexSelected(index, true, false);
                allowedIds.add(getRowId(item));
            }
        });
        selectionHandler.setChangeEvents(true);
        setSelectedRowIds(allowedIds);
    }, [items, selectionHandler, getRowId, canSelectRow]);

    const colIsResizable = React.useCallback((columnId: string) => {
        const col = fluentColumns.find(c => c.key === columnId);
        return col?.isResizable !== false;
    }, [fluentColumns]);

    const onColumnResize = React.useCallback((_ev: unknown, data: { columnId: TableRowId; width: number; }) => {
        const key = String(data.columnId);
        if (!colIsResizable(key)) return;
        columnWidths.set(key, data.width);
    }, [columnWidths, colIsResizable]);

    const styles = useGridStyles();
    const shimmerItems = React.useMemo(() => Array.from({ length: count }, (_, i) => ({ __shimmerIndex__: i })), [count]);
    const dgSelectionMode = selectionMode === SelectionMode.single ? "single" : selectionMode === SelectionMode.multiple ? "multiselect" : undefined;

    return <div style={{ position: "relative", height, overflow: "auto" }}>
        <DataGrid
            items={loaded ? items : shimmerItems}
            columns={tableColumns}
            getRowId={getRowId}
            size="small"
            sortable
            sortState={sortState}
            onSortChange={onSortChange}
            resizableColumns
            resizableColumnsOptions={{ autoFitColumns: false }}
            columnSizingOptions={columnSizingOptions}
            onColumnResize={onColumnResize}
            selectionMode={dgSelectionMode}
            selectedItems={selectedRowIds}
            onSelectionChange={loaded && dgSelectionMode ? onSelectionChange : undefined}
        >
            <DataGridHeader className={styles.header}>
                <DataGridRow selectionCell={{ className: styles.selectionCell }}>
                    {(col) => {
                        const resizable = colIsResizable(String(col.columnId));
                        return <DataGridHeaderCell className={styles.headerCell} {...(!resizable && { aside: null })}>{col.renderHeaderCell()}</DataGridHeaderCell>;
                    }}
                </DataGridRow>
            </DataGridHeader>
            <DataGridBody<any>>
                {({ item, rowId }) => {
                    const itemIndex = items.findIndex(i => getRowId(i) === rowId);
                    const overlay = (!("__shimmerIndex__" in item) && onRenderRow) ? onRenderRow({ item, itemIndex }) : null;
                    const lastColumnId = tableColumns[tableColumns.length - 1]?.columnId;
                    return <DataGridRow<any> key={rowId} style={overlay ? { position: "relative" } : undefined} selectionCell={{ className: styles.selectionCell }}>
                        {({ renderCell, columnId }) => <>
                            <DataGridCell className={styles.cell}>
                                {"__shimmerIndex__" in item
                                    ? <SkeletonItem style={{ height: "12px" }} />
                                    : renderCell(item)
                                }
                            </DataGridCell>
                            {overlay && columnId === lastColumnId && (
                                <div aria-hidden style={{ position: "absolute", inset: 0, padding: 0, pointerEvents: "none", overflow: "hidden" }}>
                                    {overlay}
                                </div>
                            )}
                        </>}
                    </DataGridRow>;
                }}
            </DataGridBody>
        </DataGrid>
    </div>;
};

interface FluentGridProps {
    data: any[],
    primaryID: string,
    alphaNumColumns?: { [id: string]: boolean },
    sort?: QuerySortItem,
    columns: FluentColumns,
    height?: string,
    selectionMode?: SelectionMode,
    defaultSelection?: any[],
    setSelection: ISelection | ((selection: any[]) => void),
    setTotal: (total: number) => void,
    refresh: RefreshTable,
    onRenderRow?: (props: IDetailsRowProps) => React.ReactNode,
    canSelectRow?: (item: any, index: number) => boolean
}

export const FluentGrid: React.FunctionComponent<FluentGridProps> = ({
    data,
    primaryID,
    alphaNumColumns,
    sort,
    columns,
    height,
    selectionMode = SelectionMode.multiple,
    defaultSelection,
    setSelection,
    setTotal,
    refresh,
    onRenderRow,
    canSelectRow
}) => {

    const constStore = useConst(() => new Memory(primaryID, alphaNumColumns));

    React.useEffect(() => {
        constStore.setData(data);
        refresh.call();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [constStore, data, /*refresh*/]);

    return <FluentStoreGrid store={constStore} columns={columns} sort={sort} start={0} count={data.length} height={height} selectionMode={selectionMode} setSelection={setSelection} setTotal={setTotal} refresh={refresh} onRenderRow={onRenderRow} canSelectRow={canSelectRow} />;
};

interface FluentPagedGridProps {
    store: BaseStore<any, any>,
    query?: QueryRequest,
    sort?: QuerySortItem,
    pageNum?: number,
    pageSize: number,
    total: number,
    columns: FluentColumns,
    height?: string,
    selectionMode?: SelectionMode,
    setSelection: ISelection | ((selection: any[]) => void),
    setTotal: (total: number) => void,
    refresh: RefreshTable,
    onRenderRow?: (props: IDetailsRowProps) => React.ReactNode,
    canSelectRow?: (item: any, index: number) => boolean
}

export const FluentPagedGrid: React.FunctionComponent<FluentPagedGridProps> = ({
    store,
    query,
    sort,
    pageNum = 1,
    pageSize,
    total,
    columns,
    height,
    selectionMode = SelectionMode.multiple,
    setSelection,
    setTotal,
    refresh,
    onRenderRow,
    canSelectRow
}) => {
    const [page, setPage] = React.useState(pageNum - 1);
    const [sortBy, setSortBy] = React.useState(sort);

    React.useEffect(() => {
        const maxPage = Math.ceil(total / pageSize) - 1;
        if (maxPage >= 0 && page > maxPage) {   //  maxPage can be -1 if total is 0
            setPage(maxPage);
        }
    }, [page, pageSize, total]);

    React.useEffect(() => {
        setSortBy(sort);
    }, [sort]);

    React.useEffect(() => {
        const _page = pageNum >= 1 ? pageNum - 1 : 0;
        setPage(_page);
    }, [pageNum]);

    return <FluentStoreGrid store={store} query={query} columns={columns} sort={sortBy} start={page * pageSize} count={pageSize} height={height} selectionMode={selectionMode} setSelection={setSelection} setTotal={setTotal} refresh={refresh} onRenderRow={onRenderRow} canSelectRow={canSelectRow} />;
};

interface FluentPagedFooterProps {
    persistID: string,
    pageNum?: number,
    total: number,
    selectionCount?: number,
    setPageNum: (pageNum: number) => void,
    setPageSize: (pageNum: number) => void
}

export const FluentPagedFooter: React.FunctionComponent<FluentPagedFooterProps> = ({
    persistID,
    pageNum = 1,
    total,
    selectionCount = 0,
    setPageNum,
    setPageSize
}) => {
    const [page, setPage] = React.useState(pageNum - 1);
    const [storedPageSize, persistPageSize] = useUserStore(`${persistID}_pageSize`, 25);
    const [pageSize, setPageSizeSync] = React.useState<number | undefined>(storedPageSize);

    React.useEffect(() => {
        setPageSizeSync(storedPageSize);
    }, [storedPageSize]);

    React.useEffect(() => {
        setPageNum(page + 1);
    }, [page, setPageNum]);

    React.useEffect(() => {
        setPageSize(pageSize);
    }, [pageSize, setPageSize]);

    React.useEffect(() => {
        const maxPage = Math.ceil(total / pageSize) - 1;
        if (maxPage >= 0 && page > maxPage) {   //  maxPage can be -1 if total is 0
            setPage(maxPage);
        }
    }, [page, pageSize, total]);

    React.useEffect(() => {
        const _page = pageNum >= 1 ? pageNum - 1 : 0;
        setPage(_page);
    }, [pageNum]);

    const pageCount = Math.ceil(total / pageSize);
    const start = total === 0 ? 0 : page === 0 ? 1 : (page * pageSize) + 1;
    const end = Math.min(pageSize * (page + 1), total);

    const visiblePages = React.useMemo(() => {
        const pages: number[] = [];
        const maxVisible = 5;
        let from = Math.max(0, page - Math.floor(maxVisible / 2));
        const to = Math.min(pageCount - 1, from + maxVisible - 1);
        from = Math.max(0, to - maxVisible + 1);
        for (let i = from; i <= to; i++) pages.push(i);
        return pages;
    }, [page, pageCount]);

    const goTo = React.useCallback((p: number) => {
        setPage(p);
        updatePage((p + 1).toString());
    }, []);

    return <div style={{ display: "flex", alignItems: "center", padding: "6px 12px 6px 6px", gap: "2px" }}>
        <span style={{ flex: "1 1 0", fontWeight: 600, marginLeft: "6px", color: tokens.colorNeutralForeground1, whiteSpace: "nowrap" }}>
            {start}–{end >= 0 ? end : 1} {nlsHPCC.Of.toLowerCase()} {total >= 0 ? total : "???"} {nlsHPCC.Rows}{selectionCount ? ` (${selectionCount} ${nlsHPCC.Selected})` : ""}
        </span>
        <Button appearance="subtle" size="small" icon={<ChevronDoubleLeft20Regular />} disabled={page === 0} onClick={() => goTo(0)} />
        <Button appearance="subtle" size="small" icon={<ChevronLeft20Regular />} disabled={page === 0} onClick={() => goTo(page - 1)} />
        {visiblePages.map(p => (
            <Button key={p} appearance={p === page ? "primary" : "subtle"} size="small" style={{ minWidth: "28px", padding: "0 4px" }} onClick={() => goTo(p)}>{p + 1}</Button>
        ))}
        <Button appearance="subtle" size="small" icon={<ChevronRight20Regular />} disabled={page >= pageCount - 1} onClick={() => goTo(page + 1)} />
        <Button appearance="subtle" size="small" icon={<ChevronDoubleRight20Regular />} disabled={page >= pageCount - 1} onClick={() => goTo(pageCount - 1)} />
        <Dropdown size="small" value={String(pageSize)} selectedOptions={[String(pageSize)]} style={{ minWidth: "80px" }}
            onOptionSelect={(_, data) => {
                const newPageSize = Number(data.optionValue);
                const newPage = Math.floor((page * (pageSize ?? 25)) / newPageSize);
                setPage(newPage);
                setPageSizeSync(newPageSize);
                persistPageSize(newPageSize);
                updatePage((newPage + 1).toString());
            }}
        >
            {[10, 25, 50, 100, 250, 500, 1000].map(n => <Option key={n} value={String(n)}>{String(n)}</Option>)}
        </Dropdown>
    </div>;
};
