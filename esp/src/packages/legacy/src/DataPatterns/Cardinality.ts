import { BreakdownTable } from "@hpcc-js/html";
import { config } from "./config";

export class Cardinality extends BreakdownTable {

    constructor(rows, showTitle: boolean = true) {
        super();
        if (showTitle) {
            this.columns(["Cardinality", ""]);
        }
        this
            .theadColumnStyles([{
                "text-align": "left",
                "font-size": config.secondaryFontSize + "px",
                "white-space": "nowrap"
            }])
            .tbodyColumnStyles([{
                "font-weight": "normal",
                "max-width": "60px",
                "overflow": "hidden",
                "text-overflow": "ellipsis",
                "white-space": "nowrap"
            }, {
                "font-size": config.secondaryFontSize + "px",
                "font-weight": "normal",
                "text-align": "right"
            }])
            .data(rows.map(row => [
                row.value.trim(),
                row.rec_count
            ]))
            ;
    }
}
