import * as React from "react";
import { Workunit, Result, WUStateID, WUInfo } from "@hpcc-js/comms";
import nlsHPCC from "src/nlsHPCC";

export function useWorkunit(wuid: string, full: boolean = false): [Workunit, WUStateID, number] {

    const [workunit, setWorkunit] = React.useState<Workunit>();
    const [state, setState] = React.useState<WUStateID>();
    const [lastUpdate, setLastUpdate] = React.useState(Date.now());

    React.useEffect(() => {
        const wu = Workunit.attach({ baseUrl: "" }, wuid);
        const handle = wu.watch(() => {
            if (full) {
                wu.refresh(true).then(() => {
                    setWorkunit(wu);
                    setState(wu.StateID);
                });
            } else {
                setState(wu.StateID);
            }
            setLastUpdate(Date.now());
        });
        setWorkunit(wu);
        setLastUpdate(Date.now());
        return () => {
            handle.release();
        };
    }, [wuid, full]);

    return [workunit, state, lastUpdate];
}

export function useWorkunitResults(wuid: string): [Result[], Workunit, WUStateID] {

    const [workunit, state] = useWorkunit(wuid);
    const [results, setResults] = React.useState<Result[]>([]);

    React.useEffect(() => {
        workunit?.fetchResults().then(results => {
            setResults(results);
        });
    }, [workunit, state]);

    return [results, workunit, state];
}

export interface Variable {
    Type: string;
    Name: string;
    Value: string;
}

export function useWorkunitVariables(wuid: string): [Variable[], Workunit, WUStateID] {

    const [workunit, state] = useWorkunit(wuid);
    const [variables, setVariables] = React.useState<Variable[]>([]);

    React.useEffect(() => {
        workunit?.fetchInfo({
            IncludeVariables: true,
            IncludeApplicationValues: true,
            IncludeDebugValues: true
        }).then(response => {
            const vars: Variable[] = response?.Workunit?.Variables?.ECLResult?.map(row => {
                return {
                    Type: nlsHPCC.ECL,
                    Name: row.Name,
                    Value: row.Value
                };
            }) || [];
            const appData: Variable[] = response?.Workunit?.ApplicationValues?.ApplicationValue.map(row => {
                return {
                    Type: row.Application,
                    Name: row.Name,
                    Value: row.Value
                };
            }) || [];
            const debugData: Variable[] = response?.Workunit?.DebugValues?.DebugValue.map(row => {
                return {
                    Type: nlsHPCC.Debug,
                    Name: row.Name,
                    Value: row.Value
                };
            }) || [];
            setVariables([...vars, ...appData, ...debugData]);
        });
    }, [workunit, state]);

    return [variables, workunit, state];
}

export interface SourceFile extends WUInfo.ECLSourceFile {
    __hpcc_parentName: string;
}

export function useWorkunitSourceFiles(wuid: string): [SourceFile[], Workunit, WUStateID] {

    const [workunit, state] = useWorkunit(wuid);
    const [sourceFiles, setSourceFiles] = React.useState<SourceFile[]>([]);

    React.useEffect(() => {
        workunit?.fetchInfo({
            IncludeSourceFiles: true
        }).then(response => {
            const sourceFiles: SourceFile[] = [];
            response?.Workunit?.SourceFiles?.ECLSourceFile.forEach(sourceFile => {
                sourceFiles.push({
                    __hpcc_parentName: "",
                    ...sourceFile
                });
                sourceFile?.ECLSourceFiles?.ECLSourceFile.forEach(childSourceFile => {
                    sourceFiles.push({
                        __hpcc_parentName: sourceFile.Name,
                        ...childSourceFile
                    });
                });
            });
            setSourceFiles(sourceFiles);
        });
    }, [workunit, state]);

    return [sourceFiles, workunit, state];
}
