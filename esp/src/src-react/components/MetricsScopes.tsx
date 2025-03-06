import nlsHPCC from "src/nlsHPCC";
import { Database } from "@hpcc-js/common";
import { splitMetric, IScope } from "@hpcc-js/comms";
import { CellFormatter, ColumnFormat, ColumnType, DBStore, RowType, Table } from "@hpcc-js/dgrid";

class ColumnFormatEx extends ColumnFormat {
    formatterFunc(): CellFormatter | undefined {
        const colIdx = this._owner.columns().indexOf("__StdDevs");

        return function (this: ColumnType, cell: any, row: RowType): string {
            return row[colIdx];
        };
    }
}

class DBStoreEx extends DBStore {

    constructor(protected _table: ScopesTable, db: Database.Grid) {
        super(db);
    }

    sort(opts) {
        this._table.sort(opts);
        return this;
    }
}

export class ScopesTable extends Table {

    constructor() {
        super();
        this._store = new DBStoreEx(this, this._db);
    }

    scopeFilterFunc(row: IScope, scopeFilter: string, matchCase: boolean): boolean {
        const filter = scopeFilter.trim();
        if (filter) {
            let field = "";
            const colonIdx = filter.indexOf(":");
            if (colonIdx > 0) {
                field = filter.substring(0, colonIdx);
            }
            if (field) {
                const rawValue: string = !matchCase ? row[field]?.toString().toLowerCase() : row[field]?.toString();
                const formattedValue: string = !matchCase ? row.__formattedProps[field]?.toString().toLowerCase() : row.__formattedProps[field]?.toString();
                const filterValue: string = !matchCase ? filter.toLowerCase() : filter;
                return (rawValue?.indexOf(filterValue.substring(colonIdx + 1)) >= 0 || formattedValue?.indexOf(filterValue.substring(colonIdx + 1)) >= 0) ?? false;
            }
            for (const field in row) {
                const rawValue: string = !matchCase ? row[field]?.toString().toLowerCase() : row[field]?.toString();
                const formattedValue: string = !matchCase ? row.__formattedProps[field]?.toString().toLowerCase() : row.__formattedProps[field]?.toString();
                const filterValue: string = !matchCase ? filter.toLowerCase() : filter;
                return (rawValue?.indexOf(filterValue) >= 0 || formattedValue?.indexOf(filterValue) >= 0) ?? false;
            }
            return false;
        }
        return true;
    }

    _rawDataMap: { [id: number]: string } = {};
    metrics(metrics: IScope[], scopeTypes: string[], properties: string[], scopeFilter: string, matchCase: boolean): this {
        this
            .columns(["##"])    //  Reset hash to force recalculation of default widths
            .columns(["##", nlsHPCC.Type, "StdDevs", nlsHPCC.Scope, ...properties, "__StdDevs"])
            .columnFormats([
                new ColumnFormatEx()
                    .column("StdDevs")
                    .paletteID("StdDevs")
                    .min(0)
                    .max(6),
                new ColumnFormat()
                    .column("__StdDevs")
                    .width(0)
            ])
            .data(metrics
                .filter(m => this.scopeFilterFunc(m, scopeFilter, matchCase))
                .filter(row => {
                    return scopeTypes.indexOf(row.type) >= 0;
                }).map((row, idx) => {
                    if (idx === 0) {
                        this._rawDataMap = {
                            0: "##", 1: "type", 2: "__StdDevs", 3: "name"
                        };
                        properties.forEach((p, idx2) => {
                            this._rawDataMap[4 + idx2] = p;
                        });
                    }
                    row.__hpcc_id = row.name;
                    return [idx, row.type, row.__StdDevs === 0 ? undefined : row.__StdDevs, row.name, ...properties.map(p => {
                        return row.__groupedProps[p]?.Value ??
                            row.__groupedProps[p]?.Max ??
                            row.__groupedProps[p]?.Avg ??
                            row.__formattedProps[p] ??
                            row[p] ??
                            "";
                    }), row.__StdDevs === 0 ? "" : row.__StdDevsSource, row];
                }))
            ;
        return this;
    }

    sort(opts) {
        const optsEx = opts.map(opt => {
            return {
                idx: opt.property,
                metricLabel: this._rawDataMap[opt.property],
                splitMetricLabel: splitMetric(this._rawDataMap[opt.property]),
                descending: opt.descending
            };
        });

        const lparamIdx = this.columns().length;
        this._db.data().sort((l, r) => {
            const llparam = l[lparamIdx];
            const rlparam = r[lparamIdx];
            for (const { idx, metricLabel, splitMetricLabel, descending } of optsEx) {
                const lval = llparam[metricLabel] ?? llparam[`${splitMetricLabel.measure}Max${splitMetricLabel.label}`] ?? llparam[`${splitMetricLabel.measure}Avg${splitMetricLabel.label}`] ?? l[idx];
                const rval = rlparam[metricLabel] ?? rlparam[`${splitMetricLabel.measure}Max${splitMetricLabel.label}`] ?? rlparam[`${splitMetricLabel.measure}Avg${splitMetricLabel.label}`] ?? r[idx];
                if ((lval === undefined && rval !== undefined) || lval < rval) return descending ? 1 : -1;
                if ((lval !== undefined && rval === undefined) || lval > rval) return descending ? -1 : 1;
            }
            return 0;
        });
        return this;
    }
}
