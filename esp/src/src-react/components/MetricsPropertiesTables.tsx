import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { Palette } from "@hpcc-js/common";
import { ColumnFormat, Table } from "@hpcc-js/dgrid";
import { formatDecimal } from "src/Utility";
import { formatTwoDigits } from "src/Session";
import nlsHPCC from "src/nlsHPCC";
import { IScopeEx } from "../hooks/metrics";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";

Palette.rainbow("StdDevs", ["white", "white", "#fff0f0", "#ffC0C0", "#ff8080", "#ff0000", "#ff0000"]);

export interface MetricsPropertiesTablesProps {
    wuid?: string;
    scopesTableColumns?: string[];
    scopes?: IScopeEx[];
}

export const MetricsPropertiesTables: React.FunctionComponent<MetricsPropertiesTablesProps> = ({
    wuid = "",
    scopesTableColumns = [],
    scopes = []
}) => {

    const sortByColumns = React.useMemo(() => {
        return ["id", "type", "name", ...scopesTableColumns];
    }, [scopesTableColumns]);

    //  Props Table  ---
    const propsTable = useConst(() => new Table()
        .columns([nlsHPCC.Property, nlsHPCC.Value, "Avg", "Min", "Max", "Delta", "StdDev", "SkewMin", "SkewMax", "NodeMin", "NodeMax", "StdDevs"])
        .columnFormats([
            new ColumnFormat()
                .column(nlsHPCC.Property)
                .paletteID("StdDevs")
                .min(0)
                .max(6)
                .valueColumn("StdDevs"),
            new ColumnFormat()
                .column("StdDevs")
                .width(0)
        ])
        .sortable(true)
    );

    React.useEffect(() => {
        const props = [];
        scopes.forEach((item, idx) => {
            const scopeProps = [];
            for (const exception of item.__exceptions ?? []) {
                scopeProps.push([exception.Severity, exception.Message, `${formatTwoDigits(+exception.Priority / 1000)}s`, "", "", "", "", "", "", "", "", 6]);
            }
            for (const key in item.__groupedProps) {
                const row = item.__groupedProps[key];
                let rowValue;
                switch (row.Key) {
                    case "Filename":
                    case "Indexname":
                        rowValue = `<a href="#/files/${row.Value}">${row.Value}</a>`;
                        break;
                    case "DefinitionList":
                        const matches = row.Value?.match(/[/\\]([^/\\]+)\((\d+),(\d+)\)/);
                        const fileName = matches ? matches[1] : null;
                        const lineNum = matches ? matches[2] : null;
                        rowValue = lineNum ? `<a href="#/workunits/${wuid}/eclsummary/${fileName}/${lineNum}">${row.Value}</a>` : row.Value;
                        break;
                    case "name":
                        const splitMetricName = row.Value.split(":");
                        const lastMetricNode = splitMetricName.pop();
                        rowValue = `<a href="#/workunits/${wuid}/metrics/${splitMetricName.join(":")}/${lastMetricNode}">${row.Value}</a>`;
                        break;
                    default:
                        rowValue = row.Value;
                }
                scopeProps.push([row.Key, rowValue, row.Avg, row.Min, row.Max, row.Delta, row.StdDev === undefined ? "" : `${row.StdDev} (${formatDecimal(row.StdDevs)}σ)`, row.SkewMin, row.SkewMax, row.NodeMin, row.NodeMax, row.StdDevs]);
            }
            scopeProps.sort((l, r) => {
                const lIdx = sortByColumns.indexOf(l[0]);
                const rIdx = sortByColumns.indexOf(r[0]);
                if (lIdx >= 0 && rIdx >= 0) {
                    return lIdx <= rIdx ? -1 : 1;
                } else if (lIdx >= 0) {
                    return -1;
                } else if (rIdx >= 0) {
                    return 1;
                }
                return 0;
            });
            if (idx < scopes.length - 1) {
                scopeProps.push(["------------------------------", "------------------------------"]);
            }
            props.push(...scopeProps);
        });

        propsTable
            .columns([])
            .columns([nlsHPCC.Property, nlsHPCC.Value, "Avg", "Min", "Max", "Delta", "StdDev", "SkewMin", "SkewMax", "NodeMin", "NodeMax", "StdDevs"])
            .data(props)
            .lazyRender()
            ;
    }, [propsTable, scopes, sortByColumns, wuid]);

    return <AutosizeHpccJSComponent widget={propsTable}></AutosizeHpccJSComponent>;
};
