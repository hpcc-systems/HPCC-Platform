import * as React from "react";
import { IScope } from "@hpcc-js/comms";
import { DuckDB } from "@hpcc-js/wasm/dist/duckdb";

type AsyncDuckDB = any;
type AsyncDuckDBConnection = any;

export function useDuckDB(): [AsyncDuckDB] {

    const [db, setDb] = React.useState<AsyncDuckDB>();

    React.useEffect(() => {
        const duckdb = DuckDB.load().then(duckdb => {
            setDb(duckdb.db);
            return duckdb;
        });

        return () => {
            duckdb?.db?.close();
        };
    }, []);

    return [db];
}

export function useDuckDBConnection(scopes: IScope[], name: string): AsyncDuckDBConnection | undefined {

    const [db] = useDuckDB();
    const [connection, setConnection] = React.useState<AsyncDuckDBConnection | undefined>(undefined);

    React.useEffect(() => {
        let c: AsyncDuckDBConnection | undefined;
        if (db) {
            db.connect().then(async connection => {
                const scopesStr = JSON.stringify(scopes.map((scope, idx) => {
                    const row = {};
                    for (const key in scope) {
                        if (key.indexOf("__") !== 0) {
                            row[key] = scope[key];
                        }
                    }
                    return row;
                }));
                await db.registerFileText(`${name}.json`, scopesStr);
                await connection.insertJSONFromPath(`${name}.json`, { name });
                await connection.close();
                c = await db.connect();
                try { //  TODO:  Move to @hpcc-js/wasm
                    await c.query("LOAD autocomplete").catch(e => {
                        console.log(e.message);
                    });
                } catch (e) {
                    console.log(e.message);
                }
                setConnection(c);
            });
        }
        return () => {
            try {
                c?.query(`DROP TABLE ${name}`);
            } finally {
                c?.close();
            }

        };
    }, [db, name, scopes]);

    return connection;
}
