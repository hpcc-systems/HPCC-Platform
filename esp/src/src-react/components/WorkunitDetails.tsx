import * as React from "react";
import { Icon, Shimmer } from "@fluentui/react";
import { WsWorkunits, WorkunitsService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { SizeMe } from "../layouts/SizeMe";
import nlsHPCC from "src/nlsHPCC";
import { wuidToDate, wuidToTime } from "src/Utility";
import { emptyFilter, formatQuery } from "src/ESPWorkunit";
import { useLogAccessInfo, useLogicalClusters } from "../hooks/platform";
import { Variable, useWorkunit, useWorkunitVariables } from "../hooks/workunit";
import { DojoAdapter } from "../layouts/DojoAdapter";
import { FullscreenFrame, FullscreenStack } from "../layouts/Fullscreen";
import { parseQuery, pushUrl, updateFullscreen } from "../util/history";
import { WorkunitPersona } from "./controls/StateIcon";
import { Helpers } from "./Helpers";
import { IFrame } from "./IFrame";
import { Logs } from "./Logs";
import { useNextPrev } from "./Menu";
import { Processes } from "./Processes";
import { Queries } from "./Queries";
import { Resources } from "./Resources";
import { Result } from "./Result";
import { Results } from "./Results";
import { WUXMLSourceEditor } from "./SourceEditor";
import { SourceFiles } from "./SourceFiles";
import { Variables } from "./Variables";
import { Workflows } from "./Workflows";
import { WorkunitSummary } from "./WorkunitSummary";
import { TabInfo, DelayLoadedPanel, OverflowTabList } from "./controls/TabbedPanes/index";
import { ECLArchive } from "./ECLArchive";
import { Metrics } from "./Metrics";

const logger = scopedLogger("src-react/components/WorkunitDetails.tsx");

const workunitService = new WorkunitsService({ baseUrl: "" });

type StringStringMap = { [key: string]: string };

interface WorkunitDetailsProps {
    wuid: string;
    parentUrl?: string;
    tab?: string;
    fullscreen?: boolean;
    state?: { outputs?: string, metrics?: { lineageSelection?: string, selection?: string[] }, resources?: string, helpers?: string, eclsummary?: any, logicalgraph?: { lineageSelection?: string, selection?: string[] } };
    queryParams?: { summary?: StringStringMap, outputs?: StringStringMap, inputs?: StringStringMap, metrics?: StringStringMap, resources?: StringStringMap, helpers?: StringStringMap, logs?: StringStringMap, logicalgraph?: StringStringMap };
}

export const WorkunitDetails: React.FunctionComponent<WorkunitDetailsProps> = ({
    wuid,
    parentUrl = "/workunits",
    tab = "summary",
    fullscreen = false,
    state,
    queryParams = {}
}) => {

    const { workunit } = useWorkunit(wuid, true);
    const [variables, , , refreshVariables] = useWorkunitVariables(wuid);
    const [targetClusters] = useLogicalClusters();
    const [targetsRoxie, setTargetsRoxie] = React.useState(false);
    const [otTraceParent, setOtTraceParent] = React.useState("");
    const [logCount, setLogCount] = React.useState<number | string>("*");
    const { logsEnabled, logsStatusMessage } = useLogAccessInfo();
    const [_nextPrev, setNextPrev] = useNextPrev();

    const query = React.useMemo(() => {
        const parentUrlParts = parentUrl.split("!");
        if (parentUrlParts.length <= 1) {
            return emptyFilter;
        }
        return parseQuery("?" + parentUrlParts[1]);
    }, [parentUrl]);

    React.useEffect(() => {
        const traceInfo: Variable = variables.filter(v => v.Name === "ottraceparent")[0];
        setOtTraceParent(traceInfo?.Value ?? "");
    }, [variables]);

    React.useEffect(() => {
        setTargetsRoxie(targetClusters?.find(c => c.Name === workunit?.Cluster)?.Type === "roxie");
    }, [targetClusters, workunit?.Cluster]);

    const nextWuid = React.useCallback((wuids: WsWorkunits.ECLWorkunit[]) => {
        let found = false;
        for (const wu of wuids) {
            if (wu.Wuid !== wuid) {
                const oldUrl = window.location.hash;
                const newUrl = oldUrl.replace(wuid, wu.Wuid);
                pushUrl(newUrl);
                found = true;
                break;
            }
        }
        if (!found) {
            // showMessageBar({ type: MessageBarType.warning, message: nlsHPCC.WorkunitNotFound });
        }
    }, [wuid]);

    React.useEffect(() => {
        setNextPrev({
            next: () => {
                const now = new Date(Date.now());
                const tomorrow = new Date(now.getTime() + (24 * 60 * 60 * 1000));
                workunitService.WUQuery(formatQuery({
                    ...query,
                    StartDate: `${wuidToDate(wuid)}T${wuidToTime(wuid)}Z`,
                    EndDate: tomorrow.toISOString(),
                    Sortby: "Wuid",
                    Descending: false,
                    Count: 2
                }) as WsWorkunits.WUQuery).then(response => {
                    nextWuid(response?.Workunits?.ECLWorkunit || []);
                }).catch(err => logger.error(err));
            },
            previous: () => {
                workunitService.WUQuery(formatQuery({
                    ...query,
                    EndDate: `${wuidToDate(wuid)}T${wuidToTime(wuid)}Z`,
                    Count: 2
                }) as WsWorkunits.WUQuery).then(response => {
                    nextWuid(response?.Workunits?.ECLWorkunit || []);
                }).catch(err => logger.error(err));
            }
        });
        return () => {
            setNextPrev(undefined);
        };
    }, [nextWuid, query, setNextPrev, wuid]);

    const onTabSelect = React.useCallback((tab: TabInfo) => {
        pushUrl(tab.__state ?? `${parentUrl}/${wuid}/${tab.id}`);
        updateFullscreen(fullscreen);
    }, [fullscreen, parentUrl, wuid]);

    const tabs = React.useMemo((): TabInfo[] => {
        return [{
            id: "summary",
            icon: <WorkunitPersona wuid={wuid} showProtected={false} showWuid={false} />,
            label: wuid
        }, {
            id: "variables",
            label: nlsHPCC.Variables,
            count: (workunit?.VariableCount || 0) + (workunit?.ApplicationValueCount || 0) + (workunit?.DebugValueCount || 0)
        }, {
            id: "outputs",
            label: nlsHPCC.Outputs,
            disabled: workunit?.Archived,
            count: workunit?.ResultCount
        }, {
            id: "inputs",
            label: nlsHPCC.Inputs,
            disabled: workunit?.Archived,
            count: workunit?.SourceFileCount
        }, {
            id: "metrics",
            label: nlsHPCC.Metrics,
            disabled: workunit?.Archived,
            count: workunit?.GraphCount
        }, {
            id: "workflows",
            label: nlsHPCC.Workflows,
            disabled: workunit?.Archived,
            count: workunit?.WorkflowCount
        }, {
            id: "processes",
            label: nlsHPCC.Processes,
            disabled: workunit?.Archived,
            count: workunit?.ECLWUProcessList?.ECLWUProcess?.length
        }, {
            id: "queries",
            label: nlsHPCC.Queries,
            icon: <Icon iconName="Search"></Icon>,
            disabled: workunit?.Archived,
        }, {
            id: "resources",
            label: nlsHPCC.Resources,
            disabled: workunit?.Archived,
            count: workunit?.ResourceURLCount
        }, {
            id: "helpers",
            label: nlsHPCC.Helpers,
            disabled: workunit?.Archived,
            count: workunit?.HelpersCount
        }, {
            id: "logs",
            label: nlsHPCC.Logs,
            count: logCount,
            tooltipText: !logsEnabled ? (logsStatusMessage || nlsHPCC.LogsDisabled) : null,
            disabled: !logsEnabled
        }, {
            id: "eclsummary",
            label: nlsHPCC.ECL
        }, {
            id: "logicalgraph",
            label: nlsHPCC.LogicalGraph,
            disabled: workunit?.Archived || !(workunit?.DebugValues?.DebugValue ?? []).some(v => v.Name === "generatelogicalgraph" && v.Value === "1"),
        }, {
            id: "xml",
            label: nlsHPCC.XML
        }];
    }, [logCount, logsEnabled, logsStatusMessage, workunit?.Archived, workunit?.ApplicationValueCount, workunit?.DebugValueCount, workunit?.DebugValues?.DebugValue, workunit?.GraphCount, workunit?.HelpersCount, workunit?.ResourceURLCount, workunit?.ResultCount, workunit?.SourceFileCount, workunit?.VariableCount, workunit?.WorkflowCount, workunit?.ECLWUProcessList?.ECLWUProcess?.length, wuid]);

    return <FullscreenFrame fullscreen={fullscreen}>
        <SizeMe>{({ size }) =>
            <div style={{ height: "100%" }}>
                <FullscreenStack fullscreen={fullscreen}>
                    <OverflowTabList tabs={tabs} selected={tab} onTabSelect={onTabSelect} size="medium" />
                </FullscreenStack>
                <DelayLoadedPanel visible={tab === "summary"} size={size}>
                    <WorkunitSummary wuid={wuid} otTraceParent={otTraceParent} />
                </DelayLoadedPanel>
                <DelayLoadedPanel visible={tab === "variables"} size={size}>
                    <Variables variables={variables} refreshData={refreshVariables} />
                </DelayLoadedPanel>
                <DelayLoadedPanel visible={tab === "outputs"} size={size}>
                    {state?.outputs ?
                        "__legacy" in queryParams.outputs ? <IFrame src={`/WsWorkunits/WUResult?Wuid=${wuid}&ResultName=${state?.outputs}`} height="99%" /> :
                            "__visualize" in queryParams.outputs ? <DojoAdapter widgetClassID="VizWidget" params={{ Wuid: wuid, Sequence: state?.outputs }} /> :
                                <Result wuid={wuid} resultName={state?.outputs} filter={queryParams.outputs} /> :
                        <Results wuid={wuid} />
                    }
                </DelayLoadedPanel>
                <DelayLoadedPanel visible={tab === "inputs"} size={size}>
                    <SourceFiles wuid={wuid} filter={queryParams.inputs} />
                </DelayLoadedPanel>
                <DelayLoadedPanel visible={tab === "metrics"} size={size}>
                    <React.Suspense fallback={
                        <>
                            <Shimmer />
                            <Shimmer />
                            <Shimmer />
                            <Shimmer />
                        </>
                    }>
                        <Metrics wuid={wuid} targetsRoxie={targetsRoxie} parentUrl={`${parentUrl}/${wuid}/metrics`} lineageSelection={state?.metrics?.lineageSelection} selection={state?.metrics?.selection} />
                    </React.Suspense>
                </DelayLoadedPanel>
                <DelayLoadedPanel visible={tab === "workflows"} size={size}>
                    <Workflows wuid={wuid} />
                </DelayLoadedPanel>
                <DelayLoadedPanel visible={tab === "processes"} size={size}>
                    <Processes wuid={wuid} />
                </DelayLoadedPanel>
                <DelayLoadedPanel visible={tab === "queries"} size={size}>
                    <Queries filter={{ WUID: wuid }} />
                </DelayLoadedPanel>
                <DelayLoadedPanel visible={tab === "resources"} size={size}>
                    <Resources wuid={wuid} parentUrl={`${parentUrl}/${wuid}/resources`} selectedResource={state?.resources} />
                </DelayLoadedPanel>
                <DelayLoadedPanel visible={tab === "helpers"} size={size}>
                    <Helpers wuid={wuid} parentUrl={`${parentUrl}/${wuid}/helpers`} selectedTreeValue={state?.helpers} />
                </DelayLoadedPanel>
                <DelayLoadedPanel visible={tab === "logs"} size={size}>
                    <Logs wuid={wuid} filter={queryParams.logs} setLogCount={setLogCount} />
                </DelayLoadedPanel>
                <DelayLoadedPanel visible={tab === "eclsummary"} size={size}>
                    <ECLArchive
                        wuid={wuid}
                        parentUrl={`${parentUrl}/${wuid}/eclsummary`}
                        selection={state?.eclsummary?.lineageSelection ?? state?.eclsummary}
                        lineNum={state?.eclsummary?.selection ? state?.eclsummary?.selection[0] : undefined}
                    />
                </DelayLoadedPanel>
                <DelayLoadedPanel visible={tab === "logicalgraph"} size={size}>
                    <React.Suspense fallback={
                        <>
                            <Shimmer />
                            <Shimmer />
                            <Shimmer />
                            <Shimmer />
                        </>
                    }>
                        <Metrics wuid={wuid} logicalGraph={true} parentUrl={`${parentUrl}/${wuid}/logicalgraph`} lineageSelection={state?.logicalgraph?.lineageSelection} selection={state?.logicalgraph?.selection} />
                    </React.Suspense>
                </DelayLoadedPanel>
                <DelayLoadedPanel visible={tab === "xml"} size={size}>
                    <WUXMLSourceEditor wuid={wuid} />
                </DelayLoadedPanel>
            </div>
        }</SizeMe>
    </FullscreenFrame>;
};
