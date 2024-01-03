import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { Workunit, DFUWorkunit, Result, WUDetails, WUStateID, WUInfo, WorkunitsService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import * as Utility from "src/Utility";
import { singletonDebounce } from "../util/throttle";
import { useCounter } from "./util";

const logger = scopedLogger("../hooks/workunit.ts");
type RefreshFunc = (full?: boolean) => Promise<Workunit>;

export function useWorkunit(wuid: string, full: boolean = false): [Workunit, WUStateID, number, boolean, RefreshFunc] {

    const [retVal, setRetVal] = React.useState<{ workunit: Workunit, state: number, lastUpdate: number, isComplete: boolean, refresh: RefreshFunc }>();

    React.useEffect(() => {
        if (wuid === undefined || wuid === null) {
            setRetVal({ workunit: undefined, state: WUStateID.NotFound, lastUpdate: Date.now(), isComplete: undefined, refresh: (full?: boolean) => Promise.resolve(undefined) });
            return;
        }
        const wu = Workunit.attach({ baseUrl: "" }, wuid);
        let active = true;
        let handle;
        const refresh = singletonDebounce(wu, "refresh");
        refresh(full, { IncludeTotalClusterTime: true })
            .then(() => {
                if (active) {
                    setRetVal({ workunit: wu, state: wu.StateID, lastUpdate: Date.now(), isComplete: wu.isComplete(), refresh });
                    handle = wu.watch(() => {
                        setRetVal({ workunit: wu, state: wu.StateID, lastUpdate: Date.now(), isComplete: wu.isComplete(), refresh });
                    });
                }
            }).catch(err => logger.error(err));

        return () => {
            active = false;
            handle?.release();
        };
    }, [wuid, full]);

    return [retVal?.workunit, retVal?.state, retVal?.lastUpdate, retVal?.isComplete, retVal?.refresh];
}

export function useWorkunitResults(wuid: string): [Result[], Workunit, WUStateID, () => void] {

    const [workunit, state] = useWorkunit(wuid);
    const [results, setResults] = React.useState<Result[]>([]);
    const [count, inc] = useCounter();

    React.useEffect(() => {
        if (workunit) {
            const fetchResults = singletonDebounce(workunit, "fetchResults");
            fetchResults().then(results => {
                setResults(results);
            }).catch(err => logger.error(err));
        }
    }, [workunit, state, count]);

    return [results, workunit, state, inc];
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

export function useWorkunitVariables(wuid: string): [Variable[], Workunit, WUStateID, () => void] {

    const [workunit, state] = useWorkunit(wuid);
    const [variables, setVariables] = React.useState<Variable[]>([]);
    const [count, inc] = useCounter();

    React.useEffect(() => {
        if (workunit) {
            const fetchInfo = singletonDebounce(workunit, "fetchInfo");
            fetchInfo({
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
            }).catch(err => logger.error(err));
        }
    }, [workunit, state, count]);

    return [variables, workunit, state, inc];
}

export interface SourceFile extends WUInfo.ECLSourceFile {
    __hpcc_parentName: string;
}

export function useWorkunitSourceFiles(wuid: string): [SourceFile[], Workunit, WUStateID, () => void] {

    const [workunit, state] = useWorkunit(wuid);
    const [sourceFiles, setSourceFiles] = React.useState<SourceFile[]>([]);
    const [count, inc] = useCounter();

    React.useEffect(() => {
        if (workunit) {
            const fetchInfo = singletonDebounce(workunit, "fetchInfo");
            fetchInfo({
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
            }).catch(err => logger.error(err));
        }
    }, [workunit, state, count]);

    return [sourceFiles, workunit, state, inc];
}

export function useWorkunitWorkflows(wuid: string): [WUInfo.ECLWorkflow[], Workunit, () => void] {

    const [workunit, state] = useWorkunit(wuid);
    const [workflows, setWorkflows] = React.useState<WUInfo.ECLWorkflow[]>([]);
    const [count, increment] = useCounter();

    React.useEffect(() => {
        if (workunit) {
            const fetchInfo = singletonDebounce(workunit, "fetchInfo");
            fetchInfo({
                IncludeWorkflows: true
            }).then(response => {
                setWorkflows(response?.Workunit?.Workflows?.ECLWorkflow || []);
            }).catch(err => logger.error(err));
        }
    }, [workunit, state, count]);

    return [workflows, workunit, increment];
}

export function useWorkunitXML(wuid: string): [string] {

    const service = useConst(() => new WorkunitsService({ baseUrl: "" }));

    const [xml, setXML] = React.useState("");

    React.useEffect(() => {
        service.WUFile({
            Wuid: wuid,
            Type: "XML"
        }).then(response => {
            setXML(response);
        }).catch(err => logger.error(err));
    }, [wuid, service]);

    return [xml];
}

export function useWorkunitExceptions(wuid: string): [WUInfo.ECLException[], Workunit, () => void] {

    const [workunit, state] = useWorkunit(wuid);
    const [exceptions, setExceptions] = React.useState<WUInfo.ECLException[]>([]);
    const [count, increment] = useCounter();

    React.useEffect(() => {
        if (workunit) {
            const fetchInfo = singletonDebounce(workunit, "fetchInfo");
            fetchInfo({
                IncludeExceptions: true
            }).then(response => {
                setExceptions(response?.Workunit?.Exceptions?.ECLException || []);
            }).catch(err => logger.error(err));
        }
    }, [workunit, state, count]);

    return [exceptions, workunit, increment];
}

export function useWorkunitResources(wuid: string): [string[], Workunit, WUStateID, () => void] {

    const [workunit, state] = useWorkunit(wuid);
    const [resources, setResources] = React.useState<string[]>([]);
    const [count, increment] = useCounter();

    React.useEffect(() => {
        if (workunit) {
            const fetchInfo = singletonDebounce(workunit, "fetchInfo");
            fetchInfo({
                IncludeResourceURLs: true
            }).then(response => {
                setResources(response?.Workunit?.ResourceURLs?.URL || []);
            }).catch(err => logger.error(err));
        }
    }, [workunit, state, count]);

    return [resources, workunit, state, increment];
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
                Description: thorLogInfo[i].ClusterGroup + "." + thorLogInfo[i].LogDate + ".log (worker " + (j + 1) + " of " + thorLogInfo[i].NumberSlaves + ")",
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

export function useWorkunitHelpers(wuid: string): [HelperRow[], () => void] {

    const [workunit, state] = useWorkunit(wuid);
    const [counter, incCounter] = useCounter();
    const [helpers, setHelpers] = React.useState<HelperRow[]>([]);

    React.useEffect(() => {
        if (workunit) {
            const fetchInfo = singletonDebounce(workunit, "fetchInfo");
            fetchInfo({
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
            }).catch(err => logger.error(err));
        }
    }, [counter, workunit, state]);

    return [helpers, incCounter];
}

export function useGlobalWorkunitNotes(): [WUDetails.Note[]] {

    const [notes, setNotes] = React.useState<WUDetails.Note[]>([]);

    React.useEffect(() => {
        const workunit = Workunit.attach({ baseUrl: "" }, "");
        const fetchDetails = singletonDebounce(workunit, "fetchDetails");
        fetchDetails({
            PropertiesToReturn: { AllNotes: true }
        }).then(scopes => {
            setNotes(scopes[0]?.Notes.Note ?? []);
        }).catch(err => logger.error(err));
    }, []);

    return [notes];

}

export function useDfuWorkunit(wuid: string, full: boolean = false): [DFUWorkunit, WUStateID, number, boolean, (full?: boolean) => Promise<DFUWorkunit>] {

    // eslint-disable-next-line func-call-spacing
    const [retVal, setRetVal] = React.useState<{ workunit: DFUWorkunit, state: number, lastUpdate: number, isComplete: boolean, refresh: (full?: boolean) => Promise<DFUWorkunit> }>();

    React.useEffect(() => {
        if (wuid === undefined || wuid === null) {
            setRetVal({ workunit: undefined, state: WUStateID.NotFound, lastUpdate: Date.now(), isComplete: undefined, refresh: () => Promise.resolve(undefined) });
            return;
        }
        const wu = DFUWorkunit.attach({ baseUrl: "" }, wuid);
        let active = true;
        let handle;
        const refresh = singletonDebounce(wu, "refresh");
        refresh(full)
            .then(() => {
                if (active) {
                    setRetVal({ workunit: wu, state: wu.State, lastUpdate: Date.now(), isComplete: wu.isComplete(), refresh });
                    handle = wu.watch(() => {
                        setRetVal({ workunit: wu, state: wu.State, lastUpdate: Date.now(), isComplete: wu.isComplete(), refresh });
                    });
                }
            }).catch(err => logger.error(err));

        return () => {
            active = false;
            handle?.release();
        };
    }, [wuid, full]);

    return [retVal?.workunit, retVal?.state, retVal?.lastUpdate, retVal?.isComplete, retVal?.refresh];
}
