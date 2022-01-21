import * as React from "react";
import { Pivot, PivotItem } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { useWorkunit } from "../hooks/workunit";
import { DojoAdapter } from "../layouts/DojoAdapter";
import { pivotItemStyle } from "../layouts/pivot";
import { pushUrl } from "../util/history";
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

    const [workunit] = useWorkunit(wuid, true);

    const resourceCount = workunit?.ResourceURLCount > 1 ? workunit?.ResourceURLCount - 1 : undefined;

    return <SizeMe monitorHeight>{({ size }) =>
        <Pivot overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab} onLinkClick={evt => pushUrl(`/workunits/${wuid}/${evt.props.itemKey}`)}>
            <PivotItem headerText={wuid} itemKey="summary" style={pivotItemStyle(size)} >
                <WorkunitSummary wuid={wuid} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Variables} itemCount={(workunit?.VariableCount || 0) + (workunit?.ApplicationValueCount || 0) + (workunit?.DebugValueCount || 0)} itemKey="variables" style={pivotItemStyle(size, 0)}>
                <Variables wuid={wuid} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Outputs} itemKey="outputs" itemCount={workunit?.ResultCount} style={pivotItemStyle(size, 0)}>
                {state ?
                    <Result wuid={wuid} resultName={state} /> :
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
                <Resources wuid={wuid} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Helpers} itemKey="helpers" itemCount={workunit?.HelpersCount} style={pivotItemStyle(size, 0)}>
                {state ?
                    <FetchEditor mode={queryParams?.mode as any} url={queryParams?.src as string} /> :
                    <Helpers wuid={wuid} />
                }
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
