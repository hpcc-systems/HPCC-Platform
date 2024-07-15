import * as React from "react";
import { CommandBarButton, Stack } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { IScope } from "@hpcc-js/comms";
import { ICompletion } from "@hpcc-js/codemirror";
import { Table } from "@hpcc-js/dgrid";
import * as Utility from "src/Utility";
import { useDuckDBConnection } from "../hooks/duckdb";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { debounce } from "../util/throttle";
import { SQLSourceEditor } from "./SourceEditor";
import nlsHPCC from "src/nlsHPCC";

const spaceRegex = new RegExp("\\s", "g");

interface MetricsDataProps {
    defaultSql: string;
    scopes: IScope[];
    onSelectionChanged: (selection: IScope[]) => void;
}

export const MetricsSQL: React.FunctionComponent<MetricsDataProps> = ({
    defaultSql,
    scopes,
    onSelectionChanged
}) => {

    const connection = useDuckDBConnection(scopes, "metrics");
    const [schema, setSchema] = React.useState<any[]>([]);
    const [sql, setSql] = React.useState<string>(defaultSql);
    const [sqlError, setSqlError] = React.useState<Error | undefined>();
    const [dirtySql, setDirtySql] = React.useState<string>(sql);
    const [data, setData] = React.useState<any[]>([]);

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
        if (columns.length === 0 && data.length === 0 && sqlError) {
            scopesTable
                .columns(["Error"])
                .data(sqlError.message.split("\n").map(line => {
                    if (line.indexOf("LINE") === 0) {
                    } else if (line.includes("^")) {
                        line = line.replace(spaceRegex, "&nbsp;");
                    }
                    return [line];
                }))
                .lazyRender()
                ;
        } else {
            scopesTable
                .columns(["##"])    //  Reset hash to force recalculation of default widths
                .columns(["##", ...columns])
                .data(data.map((row, idx) => [idx + 1, ...row]))
                .lazyRender()
                ;
        }
    }, [columns, data, sqlError, scopesTable]);

    //  Query  ---
    React.useEffect(() => {
        if (scopes.length === 0) {
            setSchema([]);
            setData([]);
        } else if (connection) {
            connection.query(`DESCRIBE ${sql}`).then(result => {
                if (connection) {
                    setSchema(result.toArray().map((row) => row.toJSON()));
                }
            }).catch(e => {
                setSchema([]);
            });

            setSqlError(undefined);
            connection.query(sql).then(result => {
                if (connection) {
                    setData(result.toArray().map((row) => {
                        return row.toArray();
                    }));
                }
            }).catch(e => {
                setSqlError(e);
                setData([]);
            }).finally(() => {
                scopesTable.noDataMessage(nlsHPCC.noDataMessage);
            });
        }
    }, [connection, scopes.length, scopesTable, sql]);

    //  Selection  ---
    const onChange = React.useCallback((newSql: string) => {
        setDirtySql(newSql);
    }, []);

    const onFetchHints = React.useCallback((cm, option): Promise<ICompletion | null> => {
        const cursor = cm.getCursor();
        const lineStr = cm.getLine(cursor.line);
        let lineEnd = cursor.ch;
        let end = cm.indexFromPos({ line: cursor.line, ch: lineEnd });
        if (connection) {
            return connection.query(`SELECT * FROM sql_auto_complete("${dirtySql.substring(0, end)}")`).then(result => {
                if (connection) {
                    const hints = result.toArray().map((row) => row.toJSON());
                    while (lineEnd < lineStr.length && /\w/.test(lineStr.charAt(lineEnd))) ++lineEnd;
                    end = cm.indexFromPos({ line: cursor.line, ch: lineEnd });
                    const suggestion_start = hints.length ? hints[0].suggestion_start : end;
                    return {
                        list: hints.map(row => row.suggestion),
                        from: cm.posFromIndex(suggestion_start),
                        to: cm.posFromIndex(end)
                    };
                }
            }).catch(e => {
                return Promise.resolve(null);
            });
        }
        return Promise.resolve(null);
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
            <Stack horizontal style={{ width: "100%", height: "80px" }}>
                <div style={{ width: "100%", height: "80px" }}>
                    <SQLSourceEditor sql={sql} toolbar={false} onSqlChange={onChange} onFetchHints={onFetchHints} onSubmit={onSubmit} ></SQLSourceEditor>
                </div>
                <CommandBarButton iconProps={{ iconName: "Play" }} onClick={() => setSql(dirtySql)} />
                <CommandBarButton disabled={data.length === 0} iconProps={{ iconName: "Copy" }} onClick={onCopy} />
                <CommandBarButton disabled={data.length === 0} iconProps={{ iconName: "Download" }} onClick={onDownload} />
            </Stack>
        }
        main={<AutosizeHpccJSComponent widget={scopesTable} ></AutosizeHpccJSComponent>}
    />;
};
