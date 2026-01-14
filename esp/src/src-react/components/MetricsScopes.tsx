import nlsHPCC from "src/nlsHPCC";
import { Database } from "@hpcc-js/common";
import { splitMetric, IScope } from "@hpcc-js/comms";
import { CellFormatter, ColumnFormat, ColumnType, DBStore, RowType, Table } from "@hpcc-js/dgrid";
import * as Utility from "src/Utility";

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

    protected _regexCache = new Map<string, RegExp>();

    constructor() {
        super();
        this._store = new DBStoreEx(this, this._db);
    }

    clearRegexCache(): void {
        this._regexCache.clear();
    }

    protected getOrCreateRegex(searchTerm: string, matchCase: boolean): RegExp {
        const cacheKey = `${searchTerm}_${matchCase}`;
        let regex = this._regexCache.get(cacheKey);
        if (!regex) {
            const escapedTerm = searchTerm.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
            regex = new RegExp(`\\b${escapedTerm}\\b`, matchCase ? "" : "i");
            this._regexCache.set(cacheKey, regex);
        }
        return regex;
    }

    scopeFilterFunc(row: IScope, scopeFilter: string, matchCase: boolean, matchWholeWord: boolean): boolean {
        const filter = scopeFilter.trim();
        if (!filter) return true;
        const normalizedFilter = matchCase ? filter : filter.toLowerCase();

        const matchText = (text: string | undefined, normalizedFilter: string): boolean => {
            if (!text) return false;

            const normalizedText = matchCase ? text : text.toLowerCase();

            if (matchWholeWord) {
                const regex = this.getOrCreateRegex(normalizedFilter, matchCase);
                return regex.test(normalizedText);
            } else {
                return normalizedText.indexOf(normalizedFilter) >= 0;
            }
        };

        const matchField = (field: string, normalizedFilter: string): boolean => {
            const rawValue = row[field]?.toString();
            const formattedValue = row.__formattedProps?.[field]?.toString();
            return matchText(rawValue, normalizedFilter) || matchText(formattedValue, normalizedFilter);
        };

        const colonIdx = filter.indexOf(":");
        if (colonIdx > 0) {
            let field = filter.substring(0, colonIdx).trim();
            // Map display names to actual field names
            switch (field) {
                case "Type":
                    field = "type";
                    break;
                case "Scope":
                    field = "name";
                    break;
            }

            const searchTerm = filter.substring(colonIdx + 1).trim();
            const normalizedSearchTerm = matchCase ? searchTerm : searchTerm.toLowerCase();
            return matchField(field, normalizedSearchTerm) || matchField("name", normalizedFilter); // fallback to scopename + normalizedFilter if no field matches
        }
        return Object.keys(row).some(field => matchField(field, normalizedFilter));
    }

    _rawDataMap: { [id: number]: string } = {};
    metrics(metrics: IScope[], scopeTypes: string[], properties: string[], scopeFilter: string, matchCase: boolean, matchWholeWord: boolean, timeFormatHumanReadable: boolean = true): this {
        this
            .columns(["##"])    //  Reset hash to force recalculation of default widths
            .columns(["##", nlsHPCC.Type, "StdDevs", nlsHPCC.Scope, ...properties, "__StdDevs"])
            .columnFormats([
                new ColumnFormatEx()
                    .column("StdDevs")
                    .min(0)
                    .max(6),
                new ColumnFormat()
                    .column("__StdDevs")
                    .width(0)
            ])
            .data(metrics
                .filter(m => this.scopeFilterFunc(m, scopeFilter, matchCase, matchWholeWord))
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
                        const value = row.__groupedProps[p]?.Value ??
                            row.__groupedProps[p]?.Max ??
                            row.__groupedProps[p]?.Avg ??
                            row.__formattedProps[p] ??
                            row[p] ??
                            "";

                        // Format time properties
                        if (p.startsWith("Time") && value !== "" && typeof value === "number") {
                            return Utility.formatDuration(value, timeFormatHumanReadable);
                        }

                        return value;
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
