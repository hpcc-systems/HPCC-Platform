import { LogicalFile, Result, Workunit } from "@hpcc-js/comms";
import { globalKeyValStore, IKeyValStore } from "../KeyValStore";

function analyzeECL(name: string, format: string) {
    return `
IMPORT STD.DataPatterns;

filePath := '~${name}';
ds := DATASET(filePath, RECORDOF(filePath, LOOKUP), ${format});
profileResults := DataPatterns.Profile(ds);
OUTPUT(profileResults, ALL, NAMED('profileResults'));
    `;
}

function optimizeECL(origName, origFormat, newFields, transformFields, newName, overwrite) {
    return `
oldName := '~${origName}';
oldLayout := RECORDOF(oldName, LOOKUP);
oldDataset := DATASET(oldName, oldLayout, ${origFormat});

NewLayout := RECORD
${newFields}
END;

NewLayout MakeNewLayout(oldLayout L) := TRANSFORM
${transformFields}
    SELF := L;
END;

newDataset := PROJECT(oldDataset, MakeNewLayout(LEFT));
OUTPUT(newDataset, , '~${newName}'${overwrite ? ", OVERWRITE" : ""});
`;
}

export class DPWorkunit {
    private readonly _lf: LogicalFile;
    private readonly _store: IKeyValStore;
    private readonly _storeID: string;
    private readonly _storeWuidID: string;

    private _wu: Workunit;
    private _resultPromise;

    constructor(nodeGroup: string, name: string) {
        this._lf = LogicalFile.attach({ baseUrl: "" }, nodeGroup, name);
        this._store = globalKeyValStore();
        this._storeID = `dp-${nodeGroup}-${name}`;
        this._storeWuidID = `${this._storeID}-wuid`;
    }

    clearCache() {
        delete this._wu;
        delete this._resultPromise;
    }

    resolveWU(): Promise<Workunit | undefined> {
        return this._store.get(this._storeWuidID).then(wuid => {
            if (this._wu && this._wu.Wuid === wuid) {
                return this._wu;
            }
            this.clearCache();
            return wuid && Workunit.attach({ baseUrl: "" }, wuid);
        }).then(wu => {
            return wu && wu.refresh();
        }).then(wu => {
            if (wu && !wu.isDeleted()) {
                this._wu = wu;
                return wu;
            }
            return undefined;
        });
    }

    refreshWU(): Promise<Workunit | undefined> {
        return this.resolveWU().then(wu => {
            if (wu) {
                return wu.refresh();
            }
            return wu;
        }).then(wu => {
            if (wu && wu.Archived) {
                return wu.restore().then(() => wu);
            }
            return wu;
        }).then(wu => {
            if (wu && !wu.isFailed()) {
                return wu;
            }
            return undefined;
        });
    }

    delete(): Promise<void> {
        return this.resolveWU().then(wu => {
            if (wu) {
                wu.delete();
            }
            this.clearCache();
            return this._store.delete(this._storeWuidID);
        });
    }

    create(target: string): Promise<Workunit> {
        return this.resolveWU().then(wu => {
            if (wu) {
                return wu;
            }
            return this._lf.fetchInfo().then(() => {
                return Workunit.submit({ baseUrl: "" }, target, analyzeECL(this._lf.Name, this._lf.ContentType));
            }).then(wu => {
                this._wu = wu;
                return this._store.set(this._storeWuidID, wu.Wuid).then(() => wu);
            });
        });
    }

    fetchResults() {
        if (!this._resultPromise) {
            if (!this._wu) {
                this._resultPromise = Promise.resolve([]);
            } else {
                this._resultPromise = this._wu.fetchResults().then(results => {
                    return results && results[0];
                }).then((result?: Result) => {
                    if (result) {
                        return result.fetchRows();
                    }
                    return [];
                });
            }
        }
        return this._resultPromise;
    }

    optimize(target: string, name: string, overwrite: boolean) {
        return Promise.all([this._lf.fetchInfo(), this.fetchResults()]).then(([lfInfo, rows]) => {
            let fields = "";
            let transformFields = "";
            rows.forEach(row => {
                if (fields.length) fields += "\n";
                if (transformFields.length) transformFields += "\n";
                fields += `    ${row.best_attribute_type} ${row.attribute};`;
                transformFields += `    SELF.${row.attribute} := (${row.best_attribute_type})L.${row.attribute};`;
            }, "");
            return Workunit.submit({ baseUrl: "" }, target, optimizeECL(this._lf.Name, this._lf.ContentType, fields, transformFields, name, overwrite));
        });
    }
}
