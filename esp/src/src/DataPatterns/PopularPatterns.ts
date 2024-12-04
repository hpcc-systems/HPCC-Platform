import { BreakdownTable } from "@hpcc-js/html";
import { config } from "./config";

const maxRowCount = 64;

export class PopularPatterns extends BreakdownTable {
    constructor(rows, showTitle: boolean = true) {
        super();
        if (showTitle) {
            this.columns(["Popular Patterns", ""]);
        }
        this
            .fontColor("inherit")
            .theadColumnStyles([{
                "font-size": config.secondaryFontSize + "px",
                "text-align": "left",
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
            .useCalculatedRowCount(false)
            .rowCount(6)
            .data(rows
                .filter((row, idx) => idx < maxRowCount)
                .map(row => [
                    row.data_pattern.trim(),
                    row.rec_count
                ])
            )
            ;
    }
}
