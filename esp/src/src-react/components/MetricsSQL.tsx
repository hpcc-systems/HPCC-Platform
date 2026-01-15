import * as React from "react";
import { CommandBarButton } from "@fluentui/react";
import { StackShim } from "@fluentui/react-migration-v8-v9";
import { useConst } from "@fluentui/react-hooks";
import { IScope } from "@hpcc-js/comms";
import { ICompletion } from "@hpcc-js/codemirror";
import { Table } from "@hpcc-js/dgrid";
import { scopedLogger } from "@hpcc-js/util";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useDuckDBConnection } from "../hooks/duckdb";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { debounce } from "../util/throttle";
import { SQLSourceEditor } from "./SourceEditor";

const logger = scopedLogger("src-react/hooks/MetricsSQL.tsx");

const spaceRegex = new RegExp("\\s", "g");
function parseSQLErrorMessage(message?: string | Array<string>): string[][] {
    if (Array.isArray(message)) {
        message = message.join("\n");
    }
    if (typeof message === "string") {
        return message.toString().split("\n").map(line => {
            if (line.indexOf("LINE") === 0) {
            } else if (line.includes("^")) {
                line = line.replace(spaceRegex, "&nbsp;");
            }
            return [line];
        });
    }
    return [];
}


interface Schema {
    column_name: string;
    column_type: string;
    default: unknown;
    extra: unknown;
    key: unknown;
    null: string;
}

interface MetricsDataProps {
    wuid: string;
    defaultSql: string;
    scopes: IScope[];
    onSelectionChanged: (selection: IScope[]) => void;
}

export const MetricsSQL: React.FunctionComponent<MetricsDataProps> = ({
    wuid,
    defaultSql,
    scopes,
    onSelectionChanged
}) => {

    const { connection } = useDuckDBConnection(scopes, `${wuid}-metrics`);
    const [schema, setSchema] = React.useState<Schema[]>([]);
    const [result, setResult] = React.useState<object[]>([]);
    const [sql, setSql] = React.useState<string>(defaultSql);
    const [sqlError, setSqlError] = React.useState<Error | undefined>();
    const [dirtySql, setDirtySql] = React.useState<string>(sql);

    //  Grid ---
    const columns = React.useMemo((): string[] => {
        const retVal: string[] = [];
        schema.forEach(col => {
            retVal.push(col.column_name);
        });
        return retVal;
    }, [schema]);

    const scopesTable = useConst(() => new Table()
        .multiSelect(true)
        .sortable(true)
        .noDataMessage(nlsHPCC.loadingMessage)
    );

    React.useEffect(() => {
        scopesTable
            .on("click", debounce((row, col, sel) => {
                if (sel) {
                    onSelectionChanged(scopesTable.selection());
                }
            }, 100), true)
            ;
    }, [onSelectionChanged, scopesTable]);

    React.useEffect(() => {
        if (columns.length === 0 && result.length === 0 && sqlError) {
            scopesTable
                .columns(["Error"])
                .data(parseSQLErrorMessage(sqlError.message))
                .lazyRender()
                ;
        } else {
            const data = result.map((row, idx) => {
                const newRow = [idx + 1];
                for (const col of columns) {
                    newRow.push(row[col]);
                }
                return newRow;
            });
            scopesTable
                .columns(["##"])    //  Reset hash to force recalculation of default widths
                .columns(["##", ...columns])
                .data(data)
                .lazyRender()
                ;
        }
    }, [columns, result, sqlError, scopesTable]);
    //  Query  ---
    React.useEffect(() => {
        if (scopes.length === 0) {
            setSchema([]);
            setResult([]);
        } else if (connection) {
            try {
                const result = JSON.parse(connection.queryToJSON(`DESCRIBE ${sql}`));
                setSchema(result);
            } catch (e) {
                setSchema([]);
            }

            try {
                setSqlError(undefined);
                const result = JSON.parse(connection.queryToJSON(sql));
                setResult(result);
            } catch (e) {
                setSqlError(e);
                setResult([]);
            } finally {
                scopesTable.noDataMessage(nlsHPCC.noDataMessage);
            }
        }
    }, [connection, scopes.length, scopesTable, sql]);

    //  Selection  ---
    const onChange = React.useCallback((newSql: string) => {
        setDirtySql(newSql);
    }, []);

    const onFetchHints = React.useCallback((cm, option): Promise<ICompletion | null> => {
        return new Promise<ICompletion | null>(resolve => {
            const cursor = cm.getCursor();
            const lineStr = cm.getLine(cursor.line);
            let lineEnd = cursor.ch;
            let end = cm.indexFromPos({ line: cursor.line, ch: lineEnd });
            if (connection) {
                try {
                    const hints = JSON.parse(connection.queryToJSON(`SELECT * FROM sql_auto_complete("${dirtySql.substring(0, end)}")`));
                    while (lineEnd < lineStr.length && /\w/.test(lineStr.charAt(lineEnd))) ++lineEnd;
                    end = cm.indexFromPos({ line: cursor.line, ch: lineEnd });
                    const suggestion_start = hints.length ? hints[0].suggestion_start : end;
                    resolve({
                        list: hints.map(row => row.suggestion),
                        from: cm.posFromIndex(suggestion_start),
                        to: cm.posFromIndex(end)
                    });
                } catch (e) {
                    logger.debug(e);
                    return resolve(null);
                }
            }
            return resolve(null);
        });
    }, [connection, dirtySql]);

    const onSubmit = React.useCallback(() => {
        setSql(dirtySql);
    }, [dirtySql]);

    const onCopy = React.useCallback(() => {
        const tsv = scopesTable.export("TSV");
        navigator?.clipboard?.writeText(tsv);
    }, [scopesTable]);

    const onDownload = React.useCallback(() => {
        const csv = scopesTable.export("CSV");
        Utility.downloadCSV(csv, "metrics.csv");
    }, [scopesTable]);

    return <HolyGrail
        header={
            <StackShim horizontal style={{ width: "100%", height: "80px" }}>
                <div style={{ width: "100%", height: "80px" }}>
                    <SQLSourceEditor sql={sql} toolbar={false} onSqlChange={onChange} onFetchHints={onFetchHints} onSubmit={onSubmit} ></SQLSourceEditor>
                </div>
                <CommandBarButton iconProps={{ iconName: "Play" }} onClick={() => setSql(dirtySql)} />
                <CommandBarButton disabled={result.length === 0} iconProps={{ iconName: "Copy" }} onClick={onCopy} />
                <CommandBarButton disabled={result.length === 0} iconProps={{ iconName: "Download" }} onClick={onDownload} />
            </StackShim>
        }
        main={<AutosizeHpccJSComponent widget={scopesTable} ></AutosizeHpccJSComponent>}
    />;
};
