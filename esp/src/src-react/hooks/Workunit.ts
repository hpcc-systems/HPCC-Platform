import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { Workunit, Result, WUStateID, WUInfo, WorkunitsService } from "@hpcc-js/comms";
import nlsHPCC from "src/nlsHPCC";
import * as Utility from "src/Utility";

export function useCounter(): [number, () => void] {

    const [counter, setCounter] = React.useState(0);

    return [counter, () => setCounter(counter + 1)];
}

export function useWorkunit(wuid: string, full: boolean = false): [Workunit, WUStateID, number] {

    const [workunit, setWorkunit] = React.useState<Workunit>();
    const [state, setState] = React.useState<WUStateID>();
    const [lastUpdate, setLastUpdate] = React.useState(Date.now());

    React.useEffect(() => {
        const wu = Workunit.attach({ baseUrl: "" }, wuid);
        const handle = wu.watch(() => {
            if (wu.StateID !== 999) {
                if (full) {
                    wu.refresh(true).then(() => {
                        setWorkunit(wu);
                        setState(wu.StateID);
                    });
                } else {
                    setState(wu.StateID);
                }
                setLastUpdate(Date.now());
            }
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

export function useWorkunitResult(wuid: string, resultName: string): [Result, Workunit, WUStateID] {

    const [results, workunit, state] = useWorkunitResults(wuid);
    const [result, setResult] = React.useState<Result>();

    React.useEffect(() => {
        setResult(results.filter(result => result.Name === resultName)[0]);
    }, [resultName, results, state]);

    return [result, workunit, state];
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

export function useWorkunitWorkflows(wuid: string): [WUInfo.ECLWorkflow[], Workunit, () => void] {

    const [workunit, state] = useWorkunit(wuid);
    const [workflows, setWorkflows] = React.useState<WUInfo.ECLWorkflow[]>([]);
    const [count, increment] = useCounter();

    React.useEffect(() => {
        workunit?.fetchInfo({
            IncludeWorkflows: true
        }).then(response => {
            setWorkflows(response?.Workunit?.Workflows?.ECLWorkflow || []);
        });
    }, [workunit, state, count]);

    return [workflows, workunit, increment];
}

export function useWorkunitXML(wuid: string): [string] {

    const service = useConst(new WorkunitsService({ baseUrl: "" }));

    const [xml, setXML] = React.useState("");

    React.useEffect(() => {
        service.WUFile({
            Wuid: wuid,
            Type: "XML"
        }).then(response => {
            setXML(response);
        });
    }, [wuid, service]);

    return [xml];
}

export function useWorkunitExceptions(wuid: string): [WUInfo.ECLException[], Workunit, () => void] {

    const [workunit, state] = useWorkunit(wuid);
    const [exceptions, setExceptions] = React.useState<WUInfo.ECLException[]>([]);
    const [count, increment] = useCounter();

    React.useEffect(() => {
        workunit?.fetchInfo({
            IncludeExceptions: true
        }).then(response => {
            setExceptions(response?.Workunit?.Exceptions?.ECLException || []);
        });
    }, [workunit, state, count]);

    return [exceptions, workunit, increment];
}

export function useWorkunitResources(wuid: string): [string[], Workunit, WUStateID] {

    const [workunit, state] = useWorkunit(wuid);
    const [resources, setResources] = React.useState<string[]>([]);

    React.useEffect(() => {
        workunit?.fetchInfo({
            IncludeResourceURLs: true
        }).then(response => {
            setResources(response?.Workunit?.ResourceURLs?.URL || []);
        });
    }, [workunit, state]);

    return [resources, workunit, state];
}

export interface HelperRow {
    id: string;
    Type: string;
    Description?: string;
    FileSize?: number;
    Orig?: any;
    workunit: Workunit;
}

function mapHelpers(workunit: Workunit, helpers: WUInfo.ECLHelpFile[] = []): HelperRow[] {
    return helpers.map((helper, i): HelperRow => {
        return {
            id: "H:" + i,
            Type: helper.Type,
            Description: Utility.pathTail(helper.Name),
            FileSize: helper.FileSize,
            Orig: helper,
            workunit
        };
    });
}

function mapThorLogInfo(workunit: Workunit, thorLogInfo: WUInfo.ThorLogInfo[] = []): HelperRow[] {
    const retVal: HelperRow[] = [];
    for (let i = 0; i < thorLogInfo.length; ++i) {
        for (let j = 0; j < thorLogInfo[i].NumberSlaves; ++j) {
            retVal.push({
                id: "T:" + i + "_" + j,
                Type: "ThorSlaveLog",
                Description: thorLogInfo[i].ClusterGroup + "." + thorLogInfo[i].LogDate + ".log (slave " + (j + 1) + " of " + thorLogInfo[i].NumberSlaves + ")",
                Orig: {
                    SlaveNumber: j + 1,
                    ...thorLogInfo[i]
                },
                workunit
            });
        }
    }
    return retVal;
}

export function useWorkunitHelpers(wuid: string): [HelperRow[]] {

    const [workunit, state] = useWorkunit(wuid);
    const [helpers, setHelpers] = React.useState<HelperRow[]>([]);

    React.useEffect(() => {
        workunit?.fetchInfo({
            IncludeHelpers: true
        }).then(response => {
            setHelpers([{
                id: "E:0",
                Type: "ECL",
                workunit
            }, {
                id: "X:0",
                Type: "Workunit XML",
                workunit
            }, ...(workunit.HasArchiveQuery ? [{
                id: "A:0",
                Type: "Archive Query",
                workunit
            }] : []),
            ...mapHelpers(workunit, response?.Workunit?.Helpers?.ECLHelpFile),
            ...mapThorLogInfo(workunit, response?.Workunit?.ThorLogList?.ThorLogInfo)
            ]);
        });
    }, [workunit, state]);

    return [helpers];
}
