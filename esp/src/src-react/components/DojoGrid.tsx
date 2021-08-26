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

export { editor, selector, tree };

const SimpleGrid = declare([ESPUtil.Grid(false, false, undefined, false, "SimpleGrid")]);
const PageSelGrid = declare([ESPUtil.Grid(true, true, undefined, false, "PageSelGrid")]);
const SelGrid = declare([ESPUtil.Grid(false, true, undefined, false, "SelGrid")]);

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
    store,
    query,
    sort,
    columns,
    getSelected,
    setGrid,
    setSelection
}) => {

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
            columns: { ...columns }
        };
        if (getSelected !== undefined) retVal.getSelected = getSelected;
        if (store !== undefined) retVal.store = store;
        if (query !== undefined) retVal.query = query;
        if (sort !== undefined) retVal.sort = sort;
        if (columns !== undefined) retVal.columns = columns;
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
