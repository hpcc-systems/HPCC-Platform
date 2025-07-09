import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { Palette } from "@hpcc-js/common";
import { ColumnFormat, Table } from "@hpcc-js/dgrid";
import { formatDecimal } from "src/Utility";
import { formatTwoDigits } from "src/Session";
import nlsHPCC from "src/nlsHPCC";
import { IScopeEx } from "../hooks/metrics";
import { useUserTheme } from "../hooks/theme";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";

Palette.rainbow("StdDevs", ["#ffffff", "#ffffff", "#fff3cd", "#ffeaa7", "#fdcb6e", "#e17055", "#e17055"]);
Palette.rainbow("StdDevsDark", ["#222222", "#222222", "#3d3520", "#4a3c1a", "#5a4a2e", "#6b2323", "#6b2323"]);

export interface MetricsPropertiesTablesProps {
    scopesTableColumns?: string[];
    scopes?: IScopeEx[];
}

export const MetricsPropertiesTables: React.FunctionComponent<MetricsPropertiesTablesProps> = ({
    scopesTableColumns = [],
    scopes = []
}) => {

    const { isDark } = useUserTheme();

    const sortByColumns = React.useMemo(() => {
        return ["id", "type", "name", ...scopesTableColumns];
    }, [scopesTableColumns]);

    //  Props Table  ---
    const propsTable = useConst(() => new Table()
        .columns([nlsHPCC.Property, nlsHPCC.Value, "Avg", "Min", "Max", "Delta", "StdDev", "SkewMin", "SkewMax", "NodeMin", "NodeMax", "StdDevs"])
        .columnFormats([
            new ColumnFormat()
                .column(nlsHPCC.Property)
                .paletteID(isDark ? "StdDevsDark" : "StdDevs")
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
        propsTable.columnFormats()[0]?.paletteID(isDark ? "StdDevsDark" : "StdDevs");
        propsTable.render();
    }, [propsTable, isDark]);

    React.useEffect(() => {
        const props = [];
        scopes.forEach((item, idx) => {
            const scopeProps = [];
            for (const exception of item.__exceptions ?? []) {
                scopeProps.push([exception.Severity, exception.Message, `${formatTwoDigits(+exception.Priority / 1000)}s`, "", "", "", "", "", "", "", "", 6]);
            }
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
