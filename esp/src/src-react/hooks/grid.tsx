import * as React from "react";
import { DetailsList, DetailsListLayoutMode, IColumn, ICommandBarItemProps, IDetailsHeaderProps, IDetailsListStyles, Selection, TooltipHost, TooltipOverflowMode } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { Memory, QueryRequest, QuerySortItem } from "src/store/Memory";
import { createCopyDownloadSelection } from "../components/Common";
import { updateSort } from "../util/history";
import { DojoGrid } from "../components/DojoGrid";
import { useDeepCallback, useDeepEffect, useDeepMemo } from "./deepHooks";

/*  ---  Debugging dependency changes  ---
 *
 *  import { useWhatChanged } from "@simbathesailor/use-what-changed";
 *
 *  useWhatChanged([count, selectionHandler, sorted, start, store, query], "count, selectionHandler, sorted, start, store, query");
 *
 */

export interface DojoColumn {
    selectorType?: string;
    label?: string;
    field?: string;
    width?: number;
    headerIcon?: string;
    headerTooltip?: string;
    sortable?: boolean;
    disabled?: boolean | ((item: any) => boolean);
    hidden?: boolean;
    formatter?: (object, value, node, options) => any;
    renderHeaderCell?: (object, value, node, options) => any;
    renderCell?: (object, value, node, options) => any;
}

export type DojoColumns = { [key: string]: DojoColumn };

interface useGridProps {
    store: any,
    query?: QueryRequest,
    sort?: QuerySortItem,
    columns: DojoColumns,
    getSelected?: () => any[],
    filename: string
}

interface useGridResponse {
    Grid: React.FunctionComponent,
    selection: any[],
    refreshTable: (clearSelection?: boolean) => void,
    copyButtons: ICommandBarItemProps[]
}

export function useGrid({
    store,
    query,
    sort,
    columns,
    getSelected,
    filename
}: useGridProps): useGridResponse {

    const constStore = useConst(store);
    const constQuery = useConst({ ...query });
    const constSort = useConst({ ...sort });
    const constColumns = useConst({ ...columns });
    const constGetSelected = useConst(() => getSelected);
    const [grid, setGrid] = React.useState<any>(undefined);
    const [selection, setSelection] = React.useState([]);

    const Grid = React.useCallback(() => <DojoGrid
        store={constStore}
        query={constQuery}
        sort={constSort}
        columns={constColumns}
        getSelected={constGetSelected}

        setGrid={setGrid}
        setSelection={setSelection} />,
        [constColumns, constGetSelected, constQuery, constSort, constStore]);

    const refreshTable = useDeepCallback((clearSelection = false) => {
        grid?.set("query", { ...query });
        if (clearSelection) {
            grid?.clearSelection();
        }
    }, [grid], [query]);

    useDeepEffect(() => {
        refreshTable();
    }, [], [query]);

    const copyButtons = React.useMemo((): ICommandBarItemProps[] => [
        ...createCopyDownloadSelection(constColumns, selection, `${filename}.csv`)
    ], [constColumns, filename, selection]);

    return { Grid, selection, refreshTable, copyButtons };
}

function tooltipItemRenderer(item: any, index: number, column: IColumn) {
    const id = `${column.key}-${index}`;
    const value = item[column.fieldName || column.key];
    return <TooltipHost id={id} content={value} overflowMode={TooltipOverflowMode.Parent}>
        {column.data.formatter ?
            <span style={{ display: "flex" }}>{column.data.formatter(value, item)}</span> :
            <span aria-describedby={id}>{value}</span>
        }
    </TooltipHost>;
}

export function updateColumnSorted(columns: IColumn[], attr: any, desc: boolean) {
    for (const column of columns) {
        const isSorted = column.key == attr;
        column.isSorted = isSorted;
        column.isSortedDescending = isSorted && desc;
    }
}

export function columnsAdapter(columns: DojoColumns): IColumn[] {
    const retVal: IColumn[] = [];
    for (const key in columns) {
        const column = columns[key];
        if (column?.selectorType === undefined && column?.hidden !== true) {
            retVal.push({
                key,
                name: column.label ?? key,
                fieldName: column.field ?? key,
                minWidth: column.width ?? 70,
                maxWidth: column.width,
                isResizable: true,
                isSorted: false,
                isSortedDescending: false,
                iconName: column.headerIcon,
                isIconOnly: !!column.headerIcon,
                data: column,
                onRender: tooltipItemRenderer
            } as IColumn);
        }
    }
    return retVal;
}

interface useFluentStoreGridProps {
    store: any,
    query?: QueryRequest,
    sort?: QuerySortItem,
    start: number,
    count: number,
    columns: DojoColumns,
    filename: string
}

interface useFluentStoreGridResponse {
    Grid: React.FunctionComponent<{ height?: string }>,
    selection: any[],
    copyButtons: ICommandBarItemProps[],
    total: number,
    refreshTable: (clearSelection?: boolean) => void
}

export const gridStyles = (height: string): Partial<IDetailsListStyles> => {
    return {
        root: {
            height,
            minHeight: height,
            maxHeight: height,
            selectors: { ".ms-DetailsHeader-cellName": { fontSize: "13.5px" } }
        },
        headerWrapper: {
            position: "sticky",
            top: 0,
            zIndex: 2,
        }
    };
};

function useFluentStoreGrid({
    store,
    query,
    sort,
    start,
    count,
    columns,
    filename
}: useFluentStoreGridProps): useFluentStoreGridResponse {

    const memoizedColumns = useDeepMemo(() => columns, [], [columns]);
    const [sorted, setSorted] = React.useState<QuerySortItem>(sort);
    const [selection, setSelection] = React.useState([]);
    const [items, setItems] = React.useState<any[]>([]);
    const [total, setTotal] = React.useState<number>(0);

    const selectionHandler = useConst(new Selection({
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
        refreshTable();
    }, [refreshTable]);

    useDeepEffect(() => {
        setSorted(sort);
    }, [], [sort]);

    const fluentColumns: IColumn[] = React.useMemo(() => {
        return columnsAdapter(memoizedColumns);
    }, [memoizedColumns]);

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

    const Grid = React.useCallback(({ height }) => <DetailsList
        compact={true}
        items={items}
        columns={fluentColumns}
        setKey="set"
        layoutMode={DetailsListLayoutMode.justified}
        selection={selectionHandler}
        isSelectedOnFocus={false}
        selectionPreservedOnEmptyClick={true}
        onItemInvoked={this._onItemInvoked}
        onColumnHeaderClick={onColumnClick}
        onRenderDetailsHeader={renderDetailsHeader}
        styles={gridStyles(height)}
    />, [fluentColumns, items, onColumnClick, renderDetailsHeader, selectionHandler]);

    const copyButtons = React.useMemo((): ICommandBarItemProps[] => [
        ...createCopyDownloadSelection(memoizedColumns, selection, `${filename}.csv`)
    ], [memoizedColumns, filename, selection]);

    return { Grid, selection, copyButtons, total, refreshTable };
}

interface useFluentGridProps {
    data: any[],
    primaryID: string,
    alphaNumColumns?: { [id: string]: boolean },
    sort?: QuerySortItem,
    columns: DojoColumns,
    filename: string
}

export function useFluentGrid({
    data,
    primaryID,
    alphaNumColumns,
    sort,
    columns,
    filename
}: useFluentGridProps): useFluentStoreGridResponse {

    const constStore = useConst(new Memory(primaryID, alphaNumColumns));
    const { Grid, selection, copyButtons, total, refreshTable } = useFluentStoreGrid({ store: constStore, columns, sort, filename, start: 0, count: data.length });

    React.useEffect(() => {
        constStore.setData(data);
        refreshTable();
    }, [constStore, data, refreshTable]);

    return { Grid, selection, copyButtons, total, refreshTable };
}

