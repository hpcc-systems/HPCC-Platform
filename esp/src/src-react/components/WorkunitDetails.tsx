import * as React from "react";
import { IPivotItemProps, Pivot, PivotItem } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { service, hasLogAccess } from "src/ESPLog";
import { useWorkunit } from "../hooks/workunit";
import { useUserTheme } from "../hooks/theme";
import { DojoAdapter } from "../layouts/DojoAdapter";
import { pivotItemStyle } from "../layouts/pivot";
import { pushUrl } from "../util/history";
import { WorkunitPersona } from "./controls/StateIcon";
import { Results } from "./Results";
import { Variables } from "./Variables";
import { SourceFiles } from "./SourceFiles";
import { Helpers } from "./Helpers";
import { Queries } from "./Queries";
import { Resources } from "./Resources";
import { FetchEditor, WUXMLSourceEditor } from "./SourceEditor";
import { Workflows } from "./Workflows";
import { Metrics } from "./Metrics";
import { WorkunitSummary } from "./WorkunitSummary";
import { Result } from "./Result";
import { Logs } from "./Logs";

interface WorkunitDetailsProps {
    wuid: string;
    tab?: string;
    state?: string;
    queryParams?: { [key: string]: string };
}

export const WorkunitDetails: React.FunctionComponent<WorkunitDetailsProps> = ({
    wuid,
    tab = "summary",
    state,
    queryParams = {}
}) => {

    const { themeV9 } = useUserTheme();
    const [workunit] = useWorkunit(wuid, true);

    const wuidPivotRenderer = React.useMemo(() => {
        return function (link?: IPivotItemProps,
            defaultRenderer?: (link?: IPivotItemProps) => JSX.Element | null) {
            if (!link || !defaultRenderer) return null;
            return <span>
                <WorkunitPersona wuid={wuid} showProtected={false} showWuid={false} />
                {defaultRenderer({ ...link, itemIcon: undefined })}
            </span>;
        };
    }, [wuid]);

    const resourceCount = workunit?.ResourceURLCount > 1 ? workunit?.ResourceURLCount - 1 : undefined;

    const [logCount, setLogCount] = React.useState<number | string>("*");
    const [logsDisabled, setLogsDisabled] = React.useState(true);
    React.useEffect(() => {
        hasLogAccess().then(response => {
            setLogsDisabled(!response);
            return response;
        }).then(hasLogAccess => {
            if (hasLogAccess) {
                service.GetLogsEx({ ...queryParams, jobId: wuid, LogLineStartFrom: 0, LogLineLimit: 10 }).then(response => {    // HPCC-27711 - Requesting LogLineLimit=1 causes issues
                    setLogCount(response.total);
                });
            }
        }).catch(() => {
            setLogsDisabled(true);
        });
    }, [queryParams, wuid]);

    return <SizeMe monitorHeight>{({ size }) =>
        <Pivot overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab} onLinkClick={evt => pushUrl(`/workunits/${wuid}/${evt.props.itemKey}`)}>
            <PivotItem headerText={wuid} itemKey="summary" style={pivotItemStyle(size)} onRenderItemLink={wuidPivotRenderer}>
                <WorkunitSummary wuid={wuid} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Variables} itemCount={(workunit?.VariableCount || 0) + (workunit?.ApplicationValueCount || 0) + (workunit?.DebugValueCount || 0)} itemKey="variables" style={pivotItemStyle(size, 0)}>
                <Variables wuid={wuid} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Outputs} itemKey="outputs" itemCount={workunit?.ResultCount} style={pivotItemStyle(size, 0)}>
                {state ?
                    <Result wuid={wuid} resultName={state} filter={queryParams} /> :
                    <Results wuid={wuid} />
                }
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Inputs} itemKey="inputs" itemCount={workunit?.SourceFileCount} style={pivotItemStyle(size, 0)}>
                <SourceFiles wuid={wuid} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Metrics} itemKey="metrics" itemCount={workunit?.GraphCount} style={pivotItemStyle(size, 0)}>
                <Metrics wuid={wuid} selection={state} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Workflows} itemKey="workflows" itemCount={workunit?.WorkflowCount} style={pivotItemStyle(size, 0)}>
                <Workflows wuid={wuid} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Queries} itemIcon="Search" itemKey="queries" style={pivotItemStyle(size, 0)}>
                <Queries filter={{ WUID: wuid }} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Resources} itemKey="resources" itemCount={resourceCount} style={pivotItemStyle(size, 0)}>
                {state ?
                    <FetchEditor mode={queryParams?.mode as any} url={queryParams?.url as string} /> :
                    <Resources wuid={wuid} preview={queryParams?.preview as any} />
                }
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Helpers} itemKey="helpers" itemCount={workunit?.HelpersCount} style={pivotItemStyle(size, 0)}>
                {state ?
                    <FetchEditor mode={queryParams?.mode as any} url={queryParams?.src as string} wuid={queryParams?.mode?.toLowerCase() === "ecl" ? wuid : ""} /> :
                    <Helpers wuid={wuid} />
                }
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Logs} itemKey="logs" itemCount={logCount} headerButtonProps={logsDisabled ? { disabled: true, style: { background: themeV9.colorNeutralBackgroundDisabled, color: themeV9.colorNeutralForegroundDisabled } } : {}} style={pivotItemStyle(size, 0)}>
                <Logs wuid={wuid} filter={queryParams} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.ECL} itemKey="eclsummary" style={pivotItemStyle(size, 0)}>
                <DojoAdapter widgetClassID="ECLArchiveWidget" params={{ Wuid: wuid }} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.XML} itemKey="xml" style={pivotItemStyle(size, 0)}>
                <WUXMLSourceEditor wuid={wuid} />
            </PivotItem>
        </Pivot>
    }</SizeMe>;
};
