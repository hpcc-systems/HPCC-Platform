import { StyledTable } from "@hpcc-js/html";
import { Grid } from "@hpcc-js/layout";
import { Html } from "@hpcc-js/other";
import { config } from "./config";

class AttributeTitle extends Html {
    constructor(row) {
        super();

        const p = 8;
        const b = 1;

        this
            .html(`<span style="
                color:${config.primaryColor};
                padding:${p}px;
                display:inline-block;
                font-size:${config.secondaryFontSize}px;
                margin-top:4px;
                border:${b}px solid ${config.secondaryColor};
                border-radius:4px;
                background-color: ${config.offwhiteColor};
                width: calc(100% - ${(p * 2) + (b * 2)}px);
            ">
                <i style="
                    font-size:${config.secondaryFontSize}px;
                    color:${config.blueColor};
                " class="fa ${row.given_attribute_type.slice(0, 6) === "string" ? "fa-font" : "fa-hashtag"}"></i>
                <b>${row.attribute}</b>
                <span style="float:right;">${row.given_attribute_type}</span>
            </span>
            <span style="padding:12px 2px;display:inline-block;font-weight: bold;font-size: 13px;">
                Optimal:
            </span>
            <span style="
                color:${config.primaryColor};
                padding:4px 8px;
                display:inline-block;
                font-size:${config.secondaryFontSize}px;
                margin-top:4px;
                border:1px solid ${config.secondaryColor};
                border-radius:4px;
                background-color: ${config.offwhiteColor};
                float:right;
            ">
                <i style="
                    font-size:${config.secondaryFontSize}px;
                    color:${config.blueColor};
                " class="fa ${row.best_attribute_type.slice(0, 6) === "string" ? "fa-font" : "fa-hashtag"}"></i>
                ${row.best_attribute_type}
            </span>
            `)
            .overflowX("hidden")
            .overflowY("hidden")
            ;
    }
}

class AttributeSummary extends StyledTable {

    constructor(row) {
        super();
        const fillRate = row.fill_rate === 100 || row.fill_rate === 0 ? row.fill_rate : row.fill_rate.toFixed(1);
        this
            .data([
                ["Cardinality", row.cardinality, "(~" + (row.cardinality / row.fill_count * 100).toFixed(0) + "%)"],
                ["Filled", row.fill_count, fillRate <= config.fillRateRedThreshold ? `(<b style="color:${config.redColor}">` + fillRate + "%</b>)" : "(" + fillRate + "%)"]
            ])
            .tbodyColumnStyles([
                { "font-weight": "bold", "font-size": config.secondaryFontSize + "px", "width": "1%" },
                { "font-weight": "normal", "font-size": config.secondaryFontSize + "px", "text-align": "right", "width": "auto" },
                { "font-weight": "normal", "font-size": config.secondaryFontSize + "px", "text-align": "left", "width": "1%" }
            ])
            ;
    }

}

export class AttributeDesc extends Grid {

    constructor(row) {
        super();
        this
            .gutter(0)
            .surfaceShadow(false)
            .surfacePadding("0")
            .surfaceBorderWidth(0)
            .setContent(0, 0, new AttributeTitle(row))
            .setContent(1, 0, new AttributeSummary(row))
            ;
    }

}
