import * as React from "react";
import { IScope } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { DuckDB } from "@hpcc-js/wasm-duckdb";

const logger = scopedLogger("src-react/hooks/duckdb.ts");

type DuckDBConnection = ReturnType<typeof DuckDB.prototype.connect>;

export function useDuckDB(): [DuckDB | undefined] {

    const [duckDB, setDuckDB] = React.useState<DuckDB | undefined>(undefined);

    React.useEffect(() => {
        DuckDB.load().then(duckdb => {
            setDuckDB(duckdb);
        });
    }, []);

    return [duckDB];
}

export function useDuckDBConnection(scopes: IScope[], name: string): { duckDB: DuckDB, connection: DuckDBConnection } | undefined {

    const normalizedName = name
        .toLowerCase()
        .replace(/[^a-z0-9_]/g, "_")
        .replace(/^(\d)/, "_$1");

    const [duckDB] = useDuckDB();
    const [connection, setConnection] = React.useState<DuckDBConnection | undefined>(undefined);

    React.useEffect(() => {
        let c: DuckDBConnection | undefined;
        if (duckDB) {
            const conn: DuckDBConnection = duckDB.connect();
            try {
                const scopesStr = JSON.stringify(scopes.map((scope, idx) => {
                    const row = {};
                    for (const key in scope) {
                        if (key.indexOf("__") !== 0) {
                            row[key] = scope[key];
                        }
                    }
                    return row;
                }));
                duckDB.registerFileString(`${normalizedName}.json`, scopesStr);
                const result = conn.queryToJSON(`CREATE TABLE metrics AS SELECT * FROM read_json('${normalizedName}.json');`);
                logger.debug("useDuckDBConnection: Loaded scopes - " + result);
            } catch (e) {
                logger.error(e);
            } finally {
                conn.delete();
            }

            c = duckDB.connect();
            try {
                const result = c.queryToJSON("LOAD autocomplete");
                logger.debug("useDuckDBConnection: Loaded autocomplete - " + result);
            } catch (e) {
                logger.error(e);
            }
            setConnection(c);
        }
        return () => {
            try {
                const result = c?.queryToJSON("DROP TABLE metrics");
                logger.debug("useDuckDBConnection: Dropped table - " + result);
            } catch (e) {
                logger.error(e);
            } finally {
                c?.delete();
                duckDB?.unregisterFile(`${normalizedName}.json`);
            }
        };
    }, [duckDB, normalizedName, scopes]);

    return { duckDB, connection };
}
