import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import * as declare from "dojo/_base/declare";
// @ts-ignore
import * as selector from "dgrid/selector";
// @ts-ignore
import * as tree from "dgrid/tree";
import * as ESPUtil from "src/ESPUtil";
import { DojoComponent } from "../layouts/DojoAdapter";

import "srcReact/components/DojoGrid.css";

export { selector, tree };

const PageSelGrid = declare([ESPUtil.Grid(true, true, undefined, false, "PageSelGrid")]);
const SelGrid = declare([ESPUtil.Grid(false, true, undefined, false, "SelGrid")]);

type GridType = "PageSel" | "Sel";

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
    setGrid: (_: any) => void;
    setSelection: (_: any[]) => void;
}

export const DojoGrid: React.FunctionComponent<DojoGridProps> = ({
    type = "PageSel",
    store,
    query = {},
    sort,
    columns,
    setGrid,
    setSelection
}) => {

    const Grid = useConst(() => {
        switch (type) {
            case "Sel":
                return SelGrid;
            case "PageSel":
            default:
                return PageSelGrid;
        }
    });

    return <DojoComponent Widget={Grid} WidgetParams={{ deselectOnRefresh: true, store, query, sort, columns: { ...columns } }} postCreate={grid => {
        grid.onSelectionChanged(() => setSelection(grid.getSelected()));
        setGrid(grid);
    }} />;
};
