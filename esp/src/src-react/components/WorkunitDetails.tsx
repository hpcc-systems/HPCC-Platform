import * as React from "react";
import { Icon } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { hasLogAccess } from "src/ESPLog";
import { useWorkunit } from "../hooks/workunit";
import { useDeepEffect } from "../hooks/deepHooks";
import { DojoAdapter } from "../layouts/DojoAdapter";
import { pushUrl } from "../util/history";
import { WorkunitPersona } from "./controls/StateIcon";
import { Helpers } from "./Helpers";
import { IFrame } from "./IFrame";
import { Logs } from "./Logs";
import { Metrics } from "./Metrics";
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

const logger = scopedLogger("src-react/components/WorkunitDetails.tsx");

type StringStringMap = { [key: string]: string };
interface WorkunitDetailsProps {
    wuid: string;
    tab?: string;
    state?: { outputs?: string, metrics?: string, resources?: string, helpers?: string };
    queryParams?: { outputs?: StringStringMap, inputs?: StringStringMap, resources?: StringStringMap, helpers?: StringStringMap, logs?: StringStringMap };
}

export const WorkunitDetails: React.FunctionComponent<WorkunitDetailsProps> = ({
    wuid,
    tab = "summary",
    state,
    queryParams = {}
}) => {

    const [workunit] = useWorkunit(wuid, true);
    const [logCount, setLogCount] = React.useState<number | string>("*");
    const [logsDisabled, setLogsDisabled] = React.useState(true);

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
        pushUrl(tab.__state ?? `/workunits/${wuid}/${tab.id}`);
    }, [wuid]);

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
                <Metrics wuid={wuid} selection={state?.metrics} />
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
                <DojoAdapter widgetClassID="ECLArchiveWidget" params={{ Wuid: wuid }} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "xml"} size={size}>
                <WUXMLSourceEditor wuid={wuid} />
            </DelayLoadedPanel>
        </div>
    }</SizeMe>;
};
