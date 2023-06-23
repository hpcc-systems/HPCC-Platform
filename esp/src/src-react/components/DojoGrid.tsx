import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import * as declare from "dojo/_base/declare";
// @ts-ignore
import * as selector from "dgrid/selector";
// @ts-ignore
import * as tree from "dgrid/tree";
// @ts-ignore
import * as editor from "dgrid/editor";
import * as ESPUtil from "src/ESPUtil";
import { DojoComponent } from "../layouts/DojoAdapter";

import "src-react-css/components/DojoGrid.css";

export type Selector<T> = (_: T, type?: string) => T;
const typedSelector = selector as Selector<any>;

export { editor, typedSelector as selector, tree };

type GridType = "PageSel" | "Sel" | "SimpleGrid";

interface DojoGridProps {
    type?: GridType;
    enablePagination?: boolean;
    enableSelection?: boolean;
    overrides?: object;
    enableCompoundColumns?: boolean;
    store: any;
    query?: any;
    sort?: any;
    columns: any;
    getSelected?: () => any[];
    setGrid: (_: any) => void;
    setSelection: (_: any[]) => void;
}

export const DojoGrid: React.FunctionComponent<DojoGridProps> = ({
    type = "PageSel",
    overrides,
    store,
    query,
    sort,
    columns,
    getSelected,
    setGrid,
    setSelection
}) => {

    const SimpleGrid = React.useMemo(() => declare([ESPUtil.Grid(false, false, overrides, false, "SimpleGrid")]), [overrides]);
    const PageSelGrid = React.useMemo(() => declare([ESPUtil.Grid(true, true, overrides, false, "PageSelGrid")]), [overrides]);
    const SelGrid = React.useMemo(() => declare([ESPUtil.Grid(false, true, overrides, false, "SelGrid")]), [overrides]);

    const Grid = useConst(() => {
        switch (type) {
            case "SimpleGrid":
                return SimpleGrid;
            case "Sel":
                return SelGrid;
            case "PageSel":
            default:
                return PageSelGrid;
        }
    });

    const params = React.useMemo(() => {
        const retVal: any = {
            deselectOnRefresh: true,
            columns: {}
        };
        if (getSelected !== undefined) retVal.getSelected = getSelected;
        if (store !== undefined) retVal.store = store;
        if (query !== undefined) retVal.query = query;
        if (sort !== undefined) retVal.sort = sort;
        if (columns !== undefined) retVal.columns = { ...columns };
        return retVal;
    }, [columns, getSelected, query, sort, store]);

    const gridSelInit = React.useCallback(grid => {
        //setSelection prop is defined (grid has selectors for rows)
        if (setSelection) {
            grid.onSelectionChanged(() => setSelection(grid.getSelected()));
        }
        setGrid(grid);
    }, [setGrid, setSelection]);

    return <DojoComponent Widget={Grid} WidgetParams={params} postCreate={gridSelInit} />;
};
