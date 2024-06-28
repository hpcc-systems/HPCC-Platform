import * as React from "react";
import { Icon, Shimmer } from "@fluentui/react";
import { WsWorkunits, WorkunitsService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { hasLogAccess } from "src/ESPLog";
import { wuidToDate, wuidToTime } from "src/Utility";
import { emptyFilter, formatQuery } from "src/ESPWorkunit";
import { useWorkunit } from "../hooks/workunit";
import { useDeepEffect } from "../hooks/deepHooks";
import { DojoAdapter } from "../layouts/DojoAdapter";
import { parseQuery, pushUrl } from "../util/history";
import { WorkunitPersona } from "./controls/StateIcon";
import { Helpers } from "./Helpers";
import { IFrame } from "./IFrame";
import { Logs } from "./Logs";
import { useNextPrev } from "./Menu";
import { Queries } from "./Queries";
import { Resources } from "./Resources";
import { Result } from "./Result";
import { Results } from "./Results";
import { FetchEditor, WUXMLSourceEditor } from "./SourceEditor";
import { SourceFiles } from "./SourceFiles";
import { Variables } from "./Variables";
import { Workflows } from "./Workflows";
import { WorkunitSummary } from "./WorkunitSummary";
import { TabInfo, DelayLoadedPanel, OverflowTabList } from "./controls/TabbedPanes/index";
import { ECLArchive } from "./ECLArchive";

const Metrics = React.lazy(() => import("./Metrics").then(mod => ({ default: mod.Metrics })));

const logger = scopedLogger("src-react/components/WorkunitDetails.tsx");

const workunitService = new WorkunitsService({ baseUrl: "" });

type StringStringMap = { [key: string]: string };

interface WorkunitDetailsProps {
    wuid: string;
    parentUrl?: string;
    tab?: string;
    state?: { outputs?: string, metrics?: string, resources?: string, helpers?: string, eclsummary?: string };
    queryParams?: { outputs?: StringStringMap, inputs?: StringStringMap, resources?: StringStringMap, helpers?: StringStringMap, logs?: StringStringMap };
}

export const WorkunitDetails: React.FunctionComponent<WorkunitDetailsProps> = ({
    wuid,
    parentUrl = "/workunits",
    tab = "summary",
    state,
    queryParams = {}
}) => {

    const [workunit] = useWorkunit(wuid, true);
    const [logCount, setLogCount] = React.useState<number | string>("*");
    const [logsDisabled, setLogsDisabled] = React.useState(true);
    const [_nextPrev, setNextPrev] = useNextPrev();

    const query = React.useMemo(() => {
        const parentUrlParts = parentUrl.split("!");
        if (parentUrlParts.length <= 1) {
            return emptyFilter;
        }
        return parseQuery("?" + parentUrlParts[1]);
    }, [parentUrl]);

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

    useDeepEffect(() => {
        hasLogAccess().then(response => {
            setLogsDisabled(!response);
            return response;
        }).catch(err => {
            logger.warning(err);
            setLogsDisabled(true);
        });
    }, [wuid], [queryParams]);

    const onTabSelect = React.useCallback((tab: TabInfo) => {
        pushUrl(tab.__state ?? `${parentUrl}/${wuid}/${tab.id}`);
    }, [parentUrl, wuid]);

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
            count: workunit?.ResultCount
        }, {
            id: "inputs",
            label: nlsHPCC.Inputs,
            count: workunit?.SourceFileCount
        }, {
            id: "metrics",
            label: nlsHPCC.Metrics,
            count: workunit?.GraphCount
        }, {
            id: "workflows",
            label: nlsHPCC.Workflows,
            count: workunit?.WorkflowCount
        }, {
            id: "queries",
            icon: <Icon iconName="Search"></Icon>,
            label: nlsHPCC.Queries
        }, {
            id: "resources",
            label: nlsHPCC.Resources,
            count: workunit?.ResourceURLCount
        }, {
            id: "helpers",
            label: nlsHPCC.Helpers,
            count: workunit?.HelpersCount
        }, {
            id: "logs",
            label: nlsHPCC.Logs,
            count: logCount,
            disabled: logsDisabled
        }, {
            id: "eclsummary",
            label: nlsHPCC.ECL
        }, {
            id: "xml",
            label: nlsHPCC.XML
        }];
    }, [logCount, logsDisabled, workunit?.ApplicationValueCount, workunit?.DebugValueCount, workunit?.GraphCount, workunit?.HelpersCount, workunit?.ResourceURLCount, workunit?.ResultCount, workunit?.SourceFileCount, workunit?.VariableCount, workunit?.WorkflowCount, wuid]);

    return <SizeMe monitorHeight>{({ size }) =>
        <div style={{ height: "100%" }}>
            <OverflowTabList tabs={tabs} selected={tab} onTabSelect={onTabSelect} size="medium" />
            <DelayLoadedPanel visible={tab === "summary"} size={size}>
                <WorkunitSummary wuid={wuid} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "variables"} size={size}>
                <Variables wuid={wuid} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "outputs"} size={size}>
                {state?.outputs ?
                    queryParams.outputs?.hasOwnProperty("__legacy") ? <IFrame src={`/WsWorkunits/WUResult?Wuid=${wuid}&ResultName=${state?.outputs}`} height="99%" /> :
                        queryParams.outputs?.hasOwnProperty("__visualize") ? <DojoAdapter widgetClassID="VizWidget" params={{ Wuid: wuid, Sequence: state?.outputs }} /> :
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
                    <Metrics wuid={wuid} selection={state?.metrics} />
                </React.Suspense>
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "workflows"} size={size}>
                <Workflows wuid={wuid} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "queries"} size={size}>
                <Queries filter={{ WUID: wuid }} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "resources"} size={size}>
                {state?.resources ?
                    <FetchEditor mode={queryParams.resources?.mode as any} url={queryParams.resources?.url as string} /> :
                    <Resources wuid={wuid} preview={queryParams.resources?.preview as any} />
                }
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "helpers"} size={size}>
                {state?.helpers ?
                    <FetchEditor mode={queryParams.helpers?.mode as any} url={queryParams.helpers?.src as string} wuid={queryParams.helpers?.mode?.toLowerCase() === "ecl" ? wuid : ""} /> :
                    <Helpers wuid={wuid} />
                }
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "logs"} size={size}>
                <Logs wuid={wuid} filter={queryParams.logs} setLogCount={setLogCount} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "eclsummary"} size={size}>
                <ECLArchive wuid={wuid} parentUrl={`${parentUrl}/${wuid}/eclsummary`} selection={state?.eclsummary} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "xml"} size={size}>
                <WUXMLSourceEditor wuid={wuid} />
            </DelayLoadedPanel>
        </div>
    }</SizeMe>;
};
