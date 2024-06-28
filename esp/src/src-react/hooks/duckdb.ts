import * as React from "react";
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

export function useDuckDBConnection<T>(scopes: T, name: string): AsyncDuckDBConnection | undefined {

    const [db] = useDuckDB();
    const [connection, setConnection] = React.useState<AsyncDuckDBConnection | undefined>(undefined);

    React.useEffect(() => {
        let c: AsyncDuckDBConnection | undefined;
        if (db) {
            db.connect().then(async connection => {
                await db.registerFileText(`${name}.json`, JSON.stringify(scopes));
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
