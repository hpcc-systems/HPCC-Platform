import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { d3Event, Palette } from "@hpcc-js/common";
import { ColumnFormat, Table } from "@hpcc-js/dgrid";
import { formatDecimal } from "src/Utility";
import { formatTwoDigits } from "src/Session";
import nlsHPCC from "src/nlsHPCC";
import { IScopeEx } from "../hooks/metrics";
import { useWorkunitArchive } from "../hooks/workunit";
import { useUserTheme } from "../hooks/theme";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { Attribute } from "../util/metricArchive";

Palette.rainbow("StdDevs", ["#ffffff", "#ffffff", "#fff3cd", "#ffeaa7", "#fdcb6e", "#e17055", "#e17055"]);
Palette.rainbow("StdDevsDark", ["#222222", "#222222", "#3d3520", "#4a3c1a", "#5a4a2e", "#6b2323", "#6b2323"]);

const DFU_WORKUNIT_REGEX = /^D\d{8}-\d{6}(?:-\d+)?$/;
const ECL_WORKUNIT_REGEX = /^W\d{8}-\d{6}(?:-\d+)?$/;

const isDfuWu = (workunitId: string): boolean => DFU_WORKUNIT_REGEX.test(workunitId);
const isEclWu = (workunitId: string): boolean => ECL_WORKUNIT_REGEX.test(workunitId);

class TableEx extends Table {
    constructor() {
        super();
    }

    enter(domNode, element) {
        super.enter(domNode, element);
        this._dgridDiv.on("dgrid-sort", () => {
            const evt = d3Event();
            if (evt.sort.length === 0) {
                const data = this.data();
                data.sort((l, r) => {
                    return l[l.length - 1] < r[r.length - 1] ? -1 : 1;
                });
                this
                    .data(data)
                    .lazyRender()
                    ;
            }
        });
    }

    update(domNode, element) {
        super.update(domNode, element);
        if (!this._dgrid.hasNeutralSort) {
            this._dgrid.hasNeutralSort = true;
        }
    }

    exit(domNode, element) {
        this._dgridDiv.on("dgrid-sort", null);
        super.exit(domNode, element);
    }
}

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

    const [, , , archive,] = useWorkunitArchive(wuid);
    const { isDark } = useUserTheme();

    const sortByColumns = React.useMemo(() => {
        return ["id", "type", "name", ...scopesTableColumns];
    }, [scopesTableColumns]);

    //  Props Table  ---
    const propsTable = useConst(() => new TableEx()
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

    const findArchiveItemByPath = React.useCallback((attributes: Attribute[] | undefined, filePath: string) => {
        if (!attributes) return null;
        return attributes.find(attr => {
            if (attr.sourcePath) {
                return filePath.includes(attr.sourcePath);
            }
            return attr.name && filePath.includes(attr.name);
        });
    }, []);

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
                let rowValue;
                switch (row.Key) {
                    case "Filename":
                    case "Indexname":
                        rowValue = `<a href="#/files/${row.Value}">${row.Value}</a>`;
                        break;
                    case "DefinitionList":
                        const matches = row.Value?.match(/[/\\]([^/\\]+)\((\d+),(\d+)\)/);
                        if (matches && archive) {
                            const fileName = matches[1];
                            const lineNum = matches[2];
                            const archiveItem = findArchiveItemByPath(archive.attributes, row.Value);
                            const selectionId = archiveItem?.id || fileName.replace(/\.[^.]+$/, "");
                            rowValue = `<a href="#/workunits/${wuid}/eclsummary/${selectionId}/${lineNum}">${row.Value}</a>`;
                        } else {
                            rowValue = row.Value;
                        } break;
                    case "id":
                        if (isDfuWu(row.Value)) {
                            rowValue = `<a href="#/dfuworkunits/${row.Value}/summary">${row.Value}</a>`;
                        } else if (isEclWu(row.Value)) {
                            rowValue = `<a href="#/workunits/${row.Value}/summary">${row.Value}</a>`;
                        } else {
                            rowValue = row.Value;
                        }
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
            for (const prop of scopeProps) {
                props.push([...prop, props.length]);
            }
        });

        propsTable
            .columns([])
            .columns([nlsHPCC.Property, nlsHPCC.Value, "Avg", "Min", "Max", "Delta", "StdDev", "SkewMin", "SkewMax", "NodeMin", "NodeMax", "StdDevs"])
            .data(props)
            .lazyRender()
            ;
    }, [archive, findArchiveItemByPath, propsTable, scopes, sortByColumns, wuid]);

    return <AutosizeHpccJSComponent widget={propsTable}></AutosizeHpccJSComponent>;
};
