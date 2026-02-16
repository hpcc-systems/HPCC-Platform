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

    _rawDataMap: { [id: number]: { property: string, subvalue?: string } } = {};
    metrics(metrics: IScope[], scopeTypes: string[], properties: string[], scopeFilter: string, matchCase: boolean, matchWholeWord: boolean): this {
        let hasStdDevs = false;
        const expandedProps = new Map<string, boolean>();
        const filteredMetrics = metrics.filter(m => this.scopeFilterFunc(m, scopeFilter, matchCase, matchWholeWord))
            .filter(row => {
                return scopeTypes.indexOf(row.type) >= 0;
            });
        filteredMetrics.forEach(row => {
            if (row.__StdDevs !== 0) {
                hasStdDevs = true;
            }
            properties.forEach(p => {
                if (!expandedProps.has(p) && (row.__groupedProps[p]?.Max !== undefined || row.__groupedProps[p]?.Avg !== undefined || row.__groupedProps[p]?.Min !== undefined)) {
                    expandedProps.set(p, true);
                }
            });
        });
        this._rawDataMap = {
            0: { property: "##" }, 1: { property: "type" }, 2: { property: "__StdDevs" }, 3: { property: "name" }
        };
        let idxOffset = 4;
        properties.forEach((p) => {
            if (expandedProps.has(p)) {
                this._rawDataMap[idxOffset++] = { property: p, subvalue: "Max" };
                this._rawDataMap[idxOffset++] = { property: p, subvalue: "Avg" };
                this._rawDataMap[idxOffset++] = { property: p, subvalue: "Min" };
            } else {
                this._rawDataMap[idxOffset++] = { property: p };
            }
        });
        const data = filteredMetrics.map((row, idx) => {
            row.__hpcc_id = row.name;
            return [idx, row.type, row.__StdDevs === 0 ? undefined : row.__StdDevs, row.name, ...properties.reduce((acc, p) => {
                if (expandedProps.has(p)) {
                    acc.push(row.__groupedProps[p]?.Max);
                    acc.push(row.__groupedProps[p]?.Avg);
                    acc.push(row.__groupedProps[p]?.Min);
                } else {
                    acc.push(row.__groupedProps[p]?.Value ??
                        row.__groupedProps[p]?.Max ??
                        row.__groupedProps[p]?.Avg ??
                        row.__formattedProps[p] ??
                        row[p] ??
                        "");
                }
                return acc;
            }, []), row.__StdDevs === 0 ? "" : row.__StdDevsSource, row];
        });

        this
            .columns(["##"])    //  Reset hash to force recalculation of default widths
            .columns(["##", nlsHPCC.Type, "StdDevs", nlsHPCC.Scope, ...properties.reduce((acc, p) => {
                if (expandedProps.has(p)) {
                    acc.push(`${p}.Max`);
                    acc.push(`${p}.Avg`);
                    acc.push(`${p}.Min`);
                } else {
                    acc.push(p);
                }
                return acc;
            }, []), "__StdDevs"])
            .columnFormats([
                new ColumnFormatEx()
                    .column("StdDevs")
                    .min(0)
                    .max(6)
                    .width(hasStdDevs ? null : 0),
                new ColumnFormat()
                    .column("__StdDevs")
                    .width(0)
            ])
            .data(data)
            ;
        return this;
    }

    sort(opts) {
        const optsEx = opts.map(opt => {
            return {
                idx: opt.property,
                splitMetricLabel: splitMetric(this._rawDataMap[opt.property].property),
                subValue: this._rawDataMap[opt.property].subvalue ?? "",
                descending: opt.descending
            };
        });

        const lparamIdx = this.columns().length;
        this._db.data().sort((l, r) => {
            const llparam = l[lparamIdx];
            const rlparam = r[lparamIdx];
            for (const { idx, splitMetricLabel, subValue, descending } of optsEx) {
                const prop = `${splitMetricLabel.measure}${subValue}${splitMetricLabel.label}`;
                const lval = llparam[prop] ?? l[idx];
                const rval = rlparam[prop] ?? r[idx];
                if ((lval === undefined && rval !== undefined) || lval < rval) return descending ? 1 : -1;
                if ((lval !== undefined && rval === undefined) || lval > rval) return descending ? -1 : 1;
            }
            return 0;
        });
        return this;
    }
}
