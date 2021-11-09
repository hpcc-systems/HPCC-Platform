import * as React from "react";
import { DetailsList, DetailsListLayoutMode, IColumn, ICommandBarItemProps, IDetailsHeaderProps, Selection } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { AlphaNumSortMemory } from "src/Memory";
import { createCopyDownloadSelection } from "../components/Common";
import { DojoGrid } from "../components/DojoGrid";
import { useDeepCallback, useDeepEffect } from "./deepHooks";

interface useGridProps {
    store: any,
    query?: object,
    sort?: object[],
    columns: object,
    getSelected?: () => any[],
    filename: string
}

export function useGrid({ store, query = {}, sort = [], columns, getSelected, filename }: useGridProps): [React.FunctionComponent, any[], (clearSelection?: boolean) => void, ICommandBarItemProps[]] {

    const constStore = useConst(store);
    const constQuery = useConst({ ...query });
    const constSort = useConst([...sort]);
    const constColumns = useConst({ ...columns });
    const constGetSelected = useConst(() => getSelected);
    const [grid, setGrid] = React.useState<any>(undefined);
    const [selection, setSelection] = React.useState([]);

    const Grid = React.useMemo(() => () => <DojoGrid
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

    return [Grid, selection, refreshTable, copyButtons];
}

interface Sorted {
    column: string;
    descending: boolean;
}

function columnsAdapter(columns, sorted: Sorted): IColumn[] {
    const retVal: IColumn[] = [];
    for (const key in columns) {
        const column = columns[key];
        if (column?.selectorType === undefined) {
            retVal.push({
                key,
                name: column.label ?? key,
                fieldName: column.field ?? key,
                minWidth: column.width,
                maxWidth: column.width,
                isResizable: true,
                isSorted: key == sorted.column,
                isSortedDescending: key == sorted.column && sorted.descending,
            } as IColumn);
        }
    }
    return retVal;
}

export function useFluentGrid({ store, query = {}, sort = [], columns, getSelected, filename }: useGridProps): [React.FunctionComponent, any[], ICommandBarItemProps[]] {

    const constQuery = useConst({ ...query });
    const constColumns = useConst({ ...columns });
    const [sorted, setSorted] = React.useState<Sorted>({ column: "", descending: false });
    const [selection, setSelection] = React.useState([]);

    const fluentColumns: IColumn[] = React.useMemo(() => {
        return columnsAdapter(constColumns, sorted);
    }, [constColumns, sorted]);

    const onColumnClick = React.useCallback((event: React.MouseEvent<HTMLElement>, column: IColumn) => {
        if (constColumns[column.key]?.sortable === false) return;

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
            column: sorted ? column.key : "",
            descending: sorted ? isSortedDescending : false
        });
    }, [constColumns]);

    const [items, setItems] = React.useState<any[]>([]);

    React.useEffect(() => {
        const sort = sorted.column ? [{ attribute: sorted.column, descending: sorted.descending }] : undefined;
        store.query(constQuery, { sort }).then(items => {
            setItems(items);
        });
    }, [constQuery, sorted.column, sorted.descending, store, store.dataVersion]);

    const selectionHandler = useConst(new Selection({
        onSelectionChanged: () => {
            setSelection(selectionHandler.getSelection());
        }
    }));

    const renderItemColumn = React.useCallback((item: any, index: number, column: IColumn) => {
        if (constColumns[column.key].formatter) {
            return <span style={{ display: "flex" }}>{constColumns[column.key].formatter(item[column.key], item)}</span>;
        }
        return <span>{item[column.key]}</span>;
    }, [constColumns]);

    const Grid = React.useMemo(() => () => <DetailsList
        compact={true}
        items={items}
        columns={fluentColumns}
        setKey="set"
        layoutMode={DetailsListLayoutMode.justified}
        onRenderItemColumn={renderItemColumn}
        selection={selectionHandler}
        selectionPreservedOnEmptyClick={true}
        onItemInvoked={this._onItemInvoked}
        onColumnHeaderClick={onColumnClick}
        styles={{ headerWrapper: { height: "auto" } }}
        onRenderDetailsHeader={(props: IDetailsHeaderProps, defaultRender?: any) => {
            return defaultRender({ ...props, styles: { root: { paddingTop: 1 } } });
        }}
    />, [fluentColumns, items, onColumnClick, renderItemColumn, selectionHandler]);

    const copyButtons = React.useMemo((): ICommandBarItemProps[] => [
        ...createCopyDownloadSelection(constColumns, selection, `${filename}.csv`)
    ], [constColumns, filename, selection]);

    return [Grid, selection, copyButtons];
}

export interface useFluentGrid2Props {
    data: any[],
    primaryID: string,
    alphaNumColumns?: { [id: string]: boolean },
    query?: object,
    sort?: object[],
    columns: object,
    getSelected?: () => any[],
    filename: string
}

export function useFluentGrid2({ data, primaryID, alphaNumColumns = {}, query = {}, sort = [], columns, getSelected, filename }: useFluentGrid2Props): [React.FunctionComponent, any[], ICommandBarItemProps[]] {

    const constStore = useConst(new AlphaNumSortMemory(primaryID, alphaNumColumns));
    const constQuery = useConst({ ...query });
    const constColumns = useConst({ ...columns });
    const [sorted, setSorted] = React.useState<Sorted>({ column: "", descending: false });
    const [selection, setSelection] = React.useState([]);
    const [items, setItems] = React.useState<any[]>([]);

    const refreshTable = React.useCallback(() => {
        const sort = sorted.column ? [{ attribute: sorted.column, descending: sorted.descending }] : undefined;
        constStore.query(constQuery, { sort }).then(items => {
            setItems(items);
        });
    }, [constQuery, constStore, sorted.column, sorted.descending]);

    React.useEffect(() => {
        refreshTable();
    }, [refreshTable]);

    React.useEffect(() => {
        constStore.setData(data);
        refreshTable();
    }, [constStore, data, refreshTable]);

    const fluentColumns: IColumn[] = React.useMemo(() => {
        return columnsAdapter(constColumns, sorted);
    }, [constColumns, sorted]);

    const onColumnClick = React.useCallback((event: React.MouseEvent<HTMLElement>, column: IColumn) => {
        if (constColumns[column.key]?.sortable === false) return;

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
            column: sorted ? column.key : "",
            descending: sorted ? isSortedDescending : false
        });
    }, [constColumns]);

    React.useEffect(() => {
        const sort = sorted.column ? [{ attribute: sorted.column, descending: sorted.descending }] : undefined;
        constStore.query(constQuery, { sort }).then(items => {
            setItems(items);
        });
    }, [constQuery, constStore, sorted.column, sorted.descending]);

    const selectionHandler = useConst(new Selection({
        onSelectionChanged: () => {
            setSelection(selectionHandler.getSelection());
        }
    }));

    const renderItemColumn = React.useCallback((item: any, index: number, column: IColumn) => {
        if (constColumns[column.key].formatter) {
            return <span style={{ display: "flex" }}>{constColumns[column.key].formatter(item[column.key], item)}</span>;
        }
        return <span>{item[column.key]}</span>;
    }, [constColumns]);

    const Grid = React.useMemo(() => () => <DetailsList
        compact={true}
        items={items}
        columns={fluentColumns}
        setKey="set"
        layoutMode={DetailsListLayoutMode.justified}
        onRenderItemColumn={renderItemColumn}
        selection={selectionHandler}
        selectionPreservedOnEmptyClick={true}
        onItemInvoked={this._onItemInvoked}
        onColumnHeaderClick={onColumnClick}
        onRenderDetailsHeader={(props: IDetailsHeaderProps, defaultRender?: any) => {
            return defaultRender({ ...props, styles: { root: { paddingTop: 1 } } });
        }}
    />, [fluentColumns, items, onColumnClick, renderItemColumn, selectionHandler]);

    const copyButtons = React.useMemo((): ICommandBarItemProps[] => [
        ...createCopyDownloadSelection(constColumns, selection, `${filename}.csv`)
    ], [constColumns, filename, selection]);

    return [Grid, selection, copyButtons];
}
