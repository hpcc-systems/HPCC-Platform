import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { Palette } from "@hpcc-js/common";
import { IScope } from "@hpcc-js/comms";
import { ColumnFormat, Table } from "@hpcc-js/dgrid";
import { formatDecimal } from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";

Palette.rainbow("StdDevs", ["white", "white", "#fff0f0", "#ffC0C0", "#ff8080", "#ff0000", "#ff0000"]);

export interface MetricsPropertiesTablesProps {
    scopesTableColumns?: string[];
    scopes?: IScope[];
}

export const MetricsPropertiesTables: React.FunctionComponent<MetricsPropertiesTablesProps> = ({
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
            for (const key in item.__groupedProps) {
                const row = item.__groupedProps[key];
                scopeProps.push([row.Key, row.Value, row.Avg, row.Min, row.Max, row.Delta, row.StdDev === undefined ? "" : `${row.StdDev} (${formatDecimal(row.StdDevs)}σ)`, row.SkewMin, row.SkewMax, row.NodeMin, row.NodeMax, row.StdDevs]);
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
    }, [propsTable, scopes, sortByColumns]);

    return <AutosizeHpccJSComponent widget={propsTable}></AutosizeHpccJSComponent>;
};
