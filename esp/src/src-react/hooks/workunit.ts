import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { Workunit, DFUWorkunit, Result, WsWorkunits, WUStateID, WorkunitsService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import * as Utility from "src/Utility";
import { singletonDebounce } from "../util/throttle";
import { useCounter } from "./util";
import { Archive } from "../util/metricArchive";

const logger = scopedLogger("../hooks/workunit.ts");
type RefreshFunc = (full?: boolean, request?: Partial<WsWorkunits.WUInfo>) => Promise<Workunit>;
interface useWorkunitResult {
    workunit: Workunit;
    state: number;
    lastUpdate: number;
    isComplete: boolean;
    refresh: RefreshFunc;
}

export function useWorkunit(wuid: string, full: boolean = false): useWorkunitResult {

    const [workunit, setWorkunit] = React.useState<Workunit>();
    const [state, setState] = React.useState<number>(WUStateID.NotFound);
    const [lastUpdate, setLastUpdate] = React.useState<number>(Date.now());
    const [isComplete, setIsComplete] = React.useState<boolean>(false);
    const [refresh, setRefresh] = React.useState<RefreshFunc>(() => (full?: boolean, request?: Partial<WsWorkunits.WUInfo>) => Promise.resolve(undefined));

    React.useEffect(() => {
        if (!wuid) {
            setWorkunit(undefined);
            setState(WUStateID.NotFound);
            setLastUpdate(Date.now());
            setIsComplete(false);
            setRefresh(() => (full?: boolean, request?: Partial<WsWorkunits.WUInfo>) => Promise.resolve(undefined));
            return;
        }

        const wu = Workunit.attach({ baseUrl: "" }, wuid);
        const doRefresh = singletonDebounce(wu, "refresh");
        setRefresh(() => (full?: boolean, request?: Partial<WsWorkunits.WUInfo>) => {
            setLastUpdate(Date.now());
            const wuInfoRequest: Partial<WsWorkunits.WUInfo> = {
                ...(request ?? {}),
                IncludeTotalClusterTime: true
            };
            return doRefresh(full, wuInfoRequest);
        });

        if (full) {
            doRefresh(full, { IncludeTotalClusterTime: true }).catch(err => logger.error(err));
        }

        let cancelled = false;
        const handle = wu.watch(() => {
            if (!cancelled) {
                setWorkunit(wu);
                setState(wu.StateID);
                setLastUpdate(Date.now());
                setIsComplete(wu.isComplete());
            }
        }, true);

        return () => {
            cancelled = true;
            handle?.release();
        };
    }, [wuid, full]);

    return { workunit, state, lastUpdate, isComplete, refresh };
}

export function useWorkunitResults(wuid: string): [Result[], Workunit, WUStateID, () => void] {

    const { workunit, state } = useWorkunit(wuid);
    const [results, setResults] = React.useState<Result[]>([]);
    const [count, inc] = useCounter();

    React.useEffect(() => {
        if (workunit) {
            const fetchResults = singletonDebounce(workunit, "fetchResults");
            fetchResults().then(results => {
                if (workunit?.ResultsDesc) {
                    setResults([]);
                    logger.error(workunit?.ResultsDesc);
                } else {
                    setResults(results);
                }
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

    const { workunit, state } = useWorkunit(wuid);
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

export interface SourceFile extends WsWorkunits.ECLSourceFile {
    __hpcc_parentName: string;
}

export function useWorkunitSourceFiles(wuid: string): [SourceFile[], Workunit, WUStateID, () => void] {

    const { workunit, state } = useWorkunit(wuid);
    const [sourceFiles, setSourceFiles] = React.useState<SourceFile[]>([]);
    const [count, inc] = useCounter();

    // sorts the WU source files alphabetically by parent name, then name
    // with children immediately following parents
    const sortFiles = React.useCallback(files => {
        const sortedFiles = [];
        const temp = files.sort((a, b) => a.Name.localeCompare(b.Name));

        temp.filter(item => item.__hpcc_parentName === "").forEach(parent => {
            sortedFiles.push(parent);
            const relatedChildren = temp.filter(child => child.__hpcc_parentName === parent.Name);
            sortedFiles.push(...relatedChildren);
        });

        return sortedFiles;
    }, []);

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
                setSourceFiles(sortFiles(sourceFiles));
            }).catch(err => logger.error(err));
        }
    }, [count, sortFiles, state, workunit]);

    return [sourceFiles, workunit, state, inc];
}

export function useWorkunitWorkflows(wuid: string): [WsWorkunits.ECLWorkflow[], Workunit, () => void] {

    const { workunit, state } = useWorkunit(wuid);
    const [workflows, setWorkflows] = React.useState<WsWorkunits.ECLWorkflow[]>([]);
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

export function useWorkunitProcesses(wuid: string): [WsWorkunits.ECLWUProcess[], Workunit, () => void] {

    const { workunit, state } = useWorkunit(wuid);
    const [processes, setProcesses] = React.useState<WsWorkunits.ECLWUProcess[]>([]);
    const [count, increment] = useCounter();

    React.useEffect(() => {
        if (workunit) {
            const fetchInfo = singletonDebounce(workunit, "fetchInfo");
            fetchInfo({
                IncludeProcesses: true
            }).then(response => {
                setProcesses(response?.Workunit?.ECLWUProcessList?.ECLWUProcess || []);
            }).catch(err => logger.error(err));
        }
    }, [workunit, state, count]);

    return [processes, workunit, increment];
}

export function useWorkunitXML(wuid: string): [string] {

    const service = useConst(() => new WorkunitsService({ baseUrl: "" }));

    const [xml, setXML] = React.useState("");

    React.useEffect(() => {
        service.WUFileEx({
            Wuid: wuid,
            Type: "XML"
        }).then(response => {
            setXML(response);
        }).catch(err => logger.error(err));
    }, [wuid, service]);

    return [xml];
}

export function useWorkunitExceptions(wuid: string): [WsWorkunits.ECLException[], Workunit, () => void] {

    const { workunit, state } = useWorkunit(wuid);
    const [exceptions, setExceptions] = React.useState<WsWorkunits.ECLException[]>([]);
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

    const { workunit, state } = useWorkunit(wuid);
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

export function useWorkunitQuery(wuid: string): [string, Workunit, WUStateID, () => void] {

    const { workunit, state } = useWorkunit(wuid);
    const [query, setQuery] = React.useState<string>("");
    const [count, increment] = useCounter();

    React.useEffect(() => {
        if (workunit) {
            const fetchQuery = singletonDebounce(workunit, "fetchQuery");
            fetchQuery().then(response => {
                setQuery(response?.Text ?? "");
            }).catch(err => logger.error(err));
        }
    }, [workunit, state, count]);

    return [query, workunit, state, increment];
}

export function useWorkunitArchive(wuid: string): [string, Workunit, WUStateID, Archive, () => void] {

    const { workunit, state } = useWorkunit(wuid);
    const [archiveString, setArchiveString] = React.useState<string>("");
    const [archive, setArchive] = React.useState<Archive>();
    const [count, increment] = useCounter();

    React.useEffect(() => {
        if (workunit) {
            const fetchArchive = singletonDebounce(workunit, "fetchArchive");
            fetchArchive().then(response => {
                setArchiveString(response);
                const archive = new Archive(response);
                setArchive(archive);
            }).catch(err => logger.error(err));
        }
    }, [workunit, state, count]);

    return [archiveString, workunit, state, archive, increment];
}

export interface HelperRow {
    id: string;
    Name?: string;
    Path?: string;
    Type: string;
    Description?: string;
    FileSize?: number;
    Orig?: any;
    workunit: Workunit;
}

function mapHelpers(workunit: Workunit, helpers: WsWorkunits.ECLHelpFile[] = []): HelperRow[] {
    return helpers.map((helper, i): HelperRow => {
        const _path = helper.Name.split("\\").join("/").split("/");
        _path.pop();
        const helperPath = _path.join("/");
        return {
            id: "H:" + i,
            Name: helper.Name,
            Type: helper.Type,
            Path: helperPath,
            Description: Utility.pathTail(helper.Name),
            FileSize: helper.FileSize,
            Orig: helper,
            workunit
        };
    });
}

function mapThorLogInfo(workunit: Workunit, thorLogInfo: WsWorkunits.ThorLogInfo[] = []): HelperRow[] {
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

    const { workunit, state } = useWorkunit(wuid);
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

export interface HelperTreeRow extends HelperRow {
    parentId: string | undefined;
}

export function useWorkunitHelpersTree(wuid: string): [HelperTreeRow[], () => void] {
    const [treeHelpers, setTreeHelpers] = React.useState<HelperTreeRow[]>([]);
    const [helpers, refresh] = useWorkunitHelpers(wuid);

    React.useEffect(() => {
        const treeHelpers: HelperTreeRow[] = [];
        const folderSet = new Set<string>();

        helpers.forEach(helper => {
            let parentFolder: string | undefined;

            if (helper.Path) {
                // Strip leading slashes to avoid empty folder names
                const helperPath = helper.Path.replace(/^\/+/, "");
                const pathDirs = helperPath.split("/");
                let currentPath = "";

                // Create folder hierarchy
                for (const dirName of pathDirs) {
                    const folderName = currentPath ? `${currentPath}/${dirName}` : dirName;
                    if (!folderSet.has(folderName)) {
                        folderSet.add(folderName);
                        treeHelpers.push({
                            ...helper,
                            Type: "folder",
                            Description: dirName,
                            id: folderName,
                            parentId: currentPath || undefined,
                        });
                    }
                    currentPath = folderName;
                }
                parentFolder = currentPath;
            }
            treeHelpers.push({
                ...helper,
                parentId: parentFolder
            });
        });

        setTreeHelpers(treeHelpers.sort((a, b) => {
            if (!a.parentId && !b.parentId) return 0;
            if (!a.parentId) return -1;
            if (!b.parentId) return 1;
            return a.parentId.localeCompare(b.parentId, undefined, { ignorePunctuation: false });
        }));
    }, [helpers]);

    return [treeHelpers, refresh];
}

export function useGlobalWorkunitNotes(): [WsWorkunits.Note[]] {

    const [notes, setNotes] = React.useState<WsWorkunits.Note[]>([]);

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
