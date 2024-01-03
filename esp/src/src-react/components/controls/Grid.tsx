import * as React from "react";
import { DetailsList, DetailsListLayoutMode, Dropdown, IColumn as _IColumn, ICommandBarItemProps, IDetailsHeaderProps, IDetailsListStyles, mergeStyleSets, Selection, Stack, TooltipHost, TooltipOverflowMode } from "@fluentui/react";
import { Pagination } from "@fluentui/react-experiments/lib/Pagination";
import { useConst } from "@fluentui/react-hooks";
import { BaseStore, Memory, QueryRequest, QuerySortItem } from "src/store/Memory";
import nlsHPCC from "src/nlsHPCC";
import { createCopyDownloadSelection } from "../Common";
import { updatePage, updateSort } from "../../util/history";
import { useDeepCallback, useDeepEffect, useDeepMemo } from "../../hooks/deepHooks";
import { useUserStore, useNonReactiveEphemeralPageStore } from "../../hooks/store";
import { useUserTheme } from "../../hooks/theme";

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
    headerIcon?: string;
    headerTooltip?: string;
    sortable?: boolean;
    disabled?: boolean | ((item: any) => boolean);
    hidden?: boolean;
    justify?: JustifyType;
    formatter?: (value: any, row: any) => any;
    className?: (value: any, row: any) => string;
}

export type FluentColumns = { [key: string]: FluentColumn };

interface IColumn extends _IColumn {
    data: FluentColumn;
}

function tooltipItemRenderer(item: any, index: number, column: IColumn) {
    const id = `${column.key}-${index}`;
    const value = item[column.fieldName || column.key];
    const className = column.data.className ? column.data.className(value, item) : "";
    const style: React.CSSProperties = {
        display: "flex",
        justifyContent: column.data.justify === "right" ? "flex-end" : "flex-start"
    };
    return <TooltipHost id={id} content={value ?? ""} overflowMode={TooltipOverflowMode.Parent}>
        {column.data.formatter ?
            <span style={style} className={className} aria-describedby={id}>{column.data.formatter(value, item) ?? ""}</span> :
            <span style={style} className={className} aria-describedby={id}>{value ?? ""}</span>
        }
    </TooltipHost>;
}

function updateColumnSorted(columns: IColumn[], attr: any, desc: boolean) {
    for (const column of columns) {
        const isSorted = column.key == attr;
        column.isSorted = isSorted;
        column.isSortedDescending = isSorted && desc;
    }
}

function columnsAdapter(columns: FluentColumns, columnWidths: Map<string, any>): IColumn[] {
    const retVal: IColumn[] = [];
    for (const key in columns) {
        const column = columns[key];
        const width = columnWidths.get(key) ?? column.width;
        if (column?.selectorType === undefined && column?.hidden !== true) {
            retVal.push({
                key,
                name: column.label ?? key,
                fieldName: column.field ?? key,
                minWidth: width ?? 70,
                maxWidth: width,
                isResizable: true,
                isSorted: false,
                isSortedDescending: false,
                iconName: column.headerIcon,
                isIconOnly: !!column.headerIcon,
                data: column,
                styles: { root: { width, ":hover": { cursor: column?.sortable !== false ? "pointer" : "default" } } },
                onRender: (item: any, index: number, col: IColumn) => {
                    col.minWidth = column.width ?? 70;
                    col.maxWidth = column.width;
                    return tooltipItemRenderer(item, index, col);
                }
            } as IColumn);
        }
    }
    return retVal;
}

const gridStyles = (height: string): Partial<IDetailsListStyles> => {
    return {
        root: {
            height,
            minHeight: height,
            maxHeight: height,
            selectors: {
                ".ms-DetailsHeader-cellName": { fontSize: "13.5px" },
                ".ms-DetailsRow-cell:has(.bgFilled)": { color: "white", boxShadow: "inset 1px 0 var(--colorNeutralBackground1), inset -1px 1px var(--colorNeutralBackground1)" },
                ".ms-DetailsRow-cell:has(.bgGreen)": { background: "green" },
                ".ms-DetailsRow-cell:has(.bgOrange)": { background: "orange" },
                ".ms-DetailsRow-cell:has(.bgRed)": { background: "red" }
            }
        },
        headerWrapper: {
            position: "sticky",
            top: 0,
            zIndex: 2,
        }
    };
};

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
    refreshTable: RefreshTable
}

export function useFluentStoreState({ page }: FluentStoreStateProps): FluentStoreStateResponse {
    const [selection, setSelection] = React.useState([]);
    const [pageNum, setPageNum] = React.useState(page);
    const [pageSize, setPageSize] = React.useState(25);
    const [total, setTotal] = React.useState(0);
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
    setSelection: (selection: any[]) => void,
    setTotal: (total: number) => void,
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
    setSelection,
    setTotal,
}) => {
    const memoizedColumns = useDeepMemo(() => columns, [], [columns]);
    const [sorted, setSorted] = React.useState<QuerySortItem>(sort);
    const [items, setItems] = React.useState<any[]>([]);
    const [columnWidths] = useNonReactiveEphemeralPageStore("columnWidths");

    const selectionHandler = useConst(() => new Selection({
        onSelectionChanged: () => {
            setSelection(selectionHandler.getSelection());
        }
    }));

    const refreshTable = useDeepCallback((clearSelection = false) => {
        if (isNaN(start) || isNaN(count)) return;
        if (clearSelection) {
            selectionHandler.setItems([], true);
        }
        const storeQuery = store.query({ ...query }, { start, count, sort: sorted ? [sorted] : undefined });
        storeQuery.total.then(total => {
            setTotal(total);
        });
        storeQuery.then(items => {
            setItems(items);
            setSelection(selectionHandler.getSelection());
        });
    }, [count, selectionHandler, start, store], [query, sorted]);

    React.useEffect(() => {
        //  Dummy line to ensure its included in the dependency array  ---
        refresh.value;
        refreshTable(refresh.clear);
    }, [refresh.clear, refresh.value, refreshTable]);

    useDeepEffect(() => {
        setSorted(sort);
    }, [], [sort]);

    const fluentColumns: IColumn[] = React.useMemo(() => {
        return columnsAdapter(memoizedColumns, columnWidths);
    }, [columnWidths, memoizedColumns]);

    React.useEffect(() => {
        updateColumnSorted(fluentColumns, sorted?.attribute as string, sorted?.descending);
    }, [fluentColumns, sorted]);

    const onColumnClick = React.useCallback((event: React.MouseEvent<HTMLElement>, column: IColumn) => {
        if (memoizedColumns[column.key]?.sortable === false) return;

        let sorted = column.isSorted;
        let isSortedDescending: boolean = column.isSortedDescending;
        if (!sorted) {
            sorted = true;
            isSortedDescending = false;
        } else if (!isSortedDescending) {
            isSortedDescending = true;
        } else {
            sorted = false;
            isSortedDescending = false;
        }
        setSorted({
            attribute: sorted ? column.key : "",
            descending: sorted ? isSortedDescending : false
        });
        updateSort(sorted, isSortedDescending, column.key);
    }, [memoizedColumns]);

    const renderDetailsHeader = React.useCallback((props: IDetailsHeaderProps, defaultRender?: any) => {
        return defaultRender({
            ...props,
            onRenderColumnHeaderTooltip: (tooltipHostProps) => {
                return <TooltipHost {...tooltipHostProps} content={tooltipHostProps?.column?.data?.headerTooltip ?? ""} />;
            },
            styles: { root: { paddingTop: 1 } }
        });
    }, []);

    const columnResize = React.useCallback((column: IColumn, newWidth: number, columnIndex?: number) => {
        columnWidths.set(column.key, newWidth);
    }, [columnWidths]);

    return <DetailsList
        compact={true}
        items={items}
        columns={fluentColumns}
        setKey="set"
        layoutMode={DetailsListLayoutMode.justified}
        selection={selectionHandler}
        isSelectedOnFocus={false}
        selectionPreservedOnEmptyClick={true}
        onColumnHeaderClick={onColumnClick}
        onRenderDetailsHeader={renderDetailsHeader}
        onColumnResize={columnResize}
        styles={gridStyles(height)}
    />;
};

interface FluentGridProps {
    data: any[],
    primaryID: string,
    alphaNumColumns?: { [id: string]: boolean },
    sort?: QuerySortItem,
    columns: FluentColumns,
    height?: string,
    setSelection: (selection: any[]) => void,
    setTotal: (total: number) => void,
    refresh: RefreshTable
}

export const FluentGrid: React.FunctionComponent<FluentGridProps> = ({
    data,
    primaryID,
    alphaNumColumns,
    sort,
    columns,
    height,
    setSelection,
    setTotal,
    refresh
}) => {

    const constStore = useConst(() => new Memory(primaryID, alphaNumColumns));

    React.useEffect(() => {
        constStore.setData(data);
        refresh.call();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [constStore, data, /*refresh*/]);

    return <FluentStoreGrid store={constStore} columns={columns} sort={sort} start={0} count={data.length} height={height} setSelection={setSelection} setTotal={setTotal} refresh={refresh}>
    </FluentStoreGrid>;
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
    setSelection: (selection: any[]) => void,
    setTotal: (total: number) => void,
    refresh: RefreshTable
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
    setSelection,
    setTotal,
    refresh
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

    return <FluentStoreGrid store={store} query={query} columns={columns} sort={sortBy} start={page * pageSize} count={pageSize} height={height} setSelection={setSelection} setTotal={setTotal} refresh={refresh}>
    </FluentStoreGrid>;
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
    const [pageSize, setPersistedPageSize] = useUserStore(`${persistID}_pageSize`, 25);
    const { theme } = useUserTheme();

    React.useEffect(() => {
        setPageNum(page + 1);
    }, [page, setPageNum]);

    React.useEffect(() => {
        setPageSize(pageSize);
    }, [pageSize, setPageSize]);

    const paginationStyles = React.useMemo(() => mergeStyleSets({
        root: {
            padding: "10px 12px 10px 6px",
            display: "grid",
            gridTemplateColumns: "9fr 1fr",
            gridColumnGap: "10px"
        },
        pageControls: {
            ".ms-Pagination-container": {
                flexDirection: "row-reverse",
                justifyContent: "space-between"
            },
            ".ms-Pagination-container > :first-child": {
                display: "flex"
            },
            ".ms-Pagination-container .ms-Button-icon": {
                color: theme.palette.themePrimary
            },
            ".ms-Pagination-container .ms-Pagination-pageNumber": {
                color: theme.palette.neutralDark
            },
            ".ms-Pagination-container button:hover": {
                backgroundColor: theme.palette.neutralLighter
            },
            ".ms-Pagination-container .is-disabled .ms-Button-icon": {
                color: theme.palette.neutralQuaternary
            }
        },
        paginationLabel: {
            fontWeight: 600,
            marginLeft: "6px",
            color: theme.palette.neutralDark,
        }
    }), [theme]);

    const dropdownChange = React.useCallback((evt, option) => {
        const newPageSize = option.key as number;
        setPage(Math.floor((page * pageSize) / newPageSize));
        setPersistedPageSize(newPageSize);
    }, [page, pageSize, setPersistedPageSize]);

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

    return <Stack horizontal className={paginationStyles.root}>
        <Stack.Item className={paginationStyles.pageControls}>
            <Pagination
                selectedPageIndex={page} itemsPerPage={pageSize} totalItemCount={total}
                pageCount={Math.ceil(total / pageSize)} format="buttons" onPageChange={index => {
                    setPage(Math.round(index));
                    updatePage(Math.round(index + 1).toString());
                }}
                onRenderVisibleItemLabel={props => {
                    const start = props.totalItemCount === 0 ? 0 : props.selectedPageIndex === 0 ? 1 : (props.selectedPageIndex * props.itemsPerPage) + 1;
                    const end = (props.itemsPerPage * (props.selectedPageIndex + 1)) > props.totalItemCount ? props.totalItemCount : props.itemsPerPage * (props.selectedPageIndex + 1);
                    return <div className={paginationStyles.paginationLabel}>
                        {start} {props.strings.divider} {end} {nlsHPCC.Of.toLowerCase()} {props.totalItemCount} {nlsHPCC.Rows} {selectionCount ? `(${selectionCount} ${nlsHPCC.Selected})` : ""}
                    </div>;
                }}
            />
        </Stack.Item>
        <Stack.Item align="center">
            <Dropdown id="pageSize" options={[
                { key: 10, text: "10" },
                { key: 25, text: "25" },
                { key: 50, text: "50" },
                { key: 100, text: "100" },
                { key: 250, text: "250" },
                { key: 500, text: "500" },
                { key: 1000, text: "1000" }
            ]} selectedKey={pageSize} onChange={dropdownChange} />
        </Stack.Item>
    </Stack>;
};
