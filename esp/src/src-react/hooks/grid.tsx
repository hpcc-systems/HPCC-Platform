import * as React from "react";
import { ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
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
        ...createCopyDownloadSelection(grid, selection, `${filename}.csv`)
    ], [filename, grid, selection]);

    return [Grid, selection, refreshTable, copyButtons];
}

// export function useMemoryGrid({ query = {}, sort = [], columns, getSelected, filename }: useGridProps): [React.FunctionComponent, any[], (clearSelection?: boolean) => void, ICommandBarItemProps[]] {
//     const [Grid, selection, refreshTable, copyButtons] = useGrid(params);
//     return [Grid, selection, refreshTable, copyButtons];
// };
