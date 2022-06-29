import * as React from "react";
import { Dropdown, DropdownMenuItemType, IDropdownStyles, IDropdownOption, Pivot, PivotItem, Stack } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { service } from "src/ESPLog";
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

    const [widget, setWidget] = React.useState();
    const [metricKey, setMetricKey] = React.useState("TimeLocalExecute");
    const [metricOptions, setMetricOptions] = React.useState([] as IDropdownOption[]);
    
    const metricDropdownStyles: Partial<IDropdownStyles> = {
        dropdown: { width: 300 },
    };

    const [workunit] = useWorkunit(wuid, true);

    const resourceCount = workunit?.ResourceURLCount > 1 ? workunit?.ResourceURLCount - 1 : undefined;

    const [logCount, setLogCount] = React.useState<number | string>("*");
    React.useEffect(() => {
        service.GetLogsEx({ ...queryParams, jobId: wuid, LogLineStartFrom: 0, LogLineLimit: 10 }).then(response => {    // HPCC-27711 - Requesting LogLineLimit=1 causes issues
            setLogCount(response.total);
        });
    }, [queryParams, wuid]);

    const widgetParams = { Wuid: wuid, metricKey, ondatachange: (data)=>{
        const subtypeMap = {};
        Object.keys(data.metricTypeMap).forEach(k=>{
            Object.keys(data.metricTypeMap[k]).forEach(k2=>{
                if(!subtypeMap[k2])subtypeMap[k2] = [];
                data.metricTypeMap[k][k2].forEach(n=>{
                    subtypeMap[k2].push(n.text);
                });
            });
        });
        const _metricOptions = data.optionArr.map(n=>{
            if(n.itemType){
                n.itemType = DropdownMenuItemType[n.itemType];
            } else {
                const prefix = data.metricSubTypes[n.text];
                n.key = `${prefix}${n.text}`;
            }
            return n;
        });
        setMetricOptions(_metricOptions);
    }};    return <SizeMe monitorHeight>{({ size }) =>
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
            <PivotItem headerText={nlsHPCC.Logs} itemKey="logs" itemCount={logCount} style={pivotItemStyle(size, 0)}>
                <Logs wuid={wuid} filter={queryParams} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.ECL} itemKey="eclsummary" style={pivotItemStyle(size, 0)}>
                <Stack style={{ height: "100%", padding: 12 }}>
                    <Stack.Item>
                        <Stack horizontal style={{ height: 75, padding: 12 }}>
                            <Stack.Item>
                                <Dropdown
                                    placeholder="Select a metric"
                                    label="List of line marker metrics"
                                    options={metricOptions}
                                    styles={metricDropdownStyles}
                                    defaultSelectedKey={metricKey}
                                    onChange={(ev, value) => {
                                        setMetricKey(value.key as string);
                                        if(widget) {
                                            const dt = (widget as any).directoryTree;
                                            const d = dt.flattenData(dt.data()).find(n=>n.selected);
                                            widgetParams.metricKey = value.key as string;
                                            (widget as any).updateMetrics(widgetParams);
                                            dt.rowClick(d.content, d.markers);
                                        }
                                    }}
                                />
                            </Stack.Item>
                        </Stack>
                    </Stack.Item>
                    <Stack.Item grow={1}>
                        <DojoAdapter
                            widgetClassID="ECLArchiveWidget"
                            onWidgetMount={w => {
                                setWidget(w);
                            }}
                            params={widgetParams}
                        />
                    </Stack.Item>
                </Stack>
            </PivotItem>
            <PivotItem headerText={nlsHPCC.XML} itemKey="xml" style={pivotItemStyle(size, 0)}>
                <WUXMLSourceEditor wuid={wuid} />
            </PivotItem>
        </Pivot>
    }</SizeMe>;
};
