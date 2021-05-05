import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, mergeStyleSets, Pivot, PivotItem, ScrollablePane, ScrollbarVisibility, Sticky, StickyPositionType } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import { ReflexContainer, ReflexSplitter, ReflexElement } from "react-reflex";
import nlsHPCC from "src/nlsHPCC";
import { getImageURL } from "src/Utility";
import { getStateIconClass } from "src/ESPWorkunit";
import { WUStatus } from "src/react/index";
import { useWorkunit } from "../hooks/Workunit";
import { DojoAdapter } from "../layouts/DojoAdapter";
import { pushUrl } from "../util/history";
import { ShortVerticalDivider } from "./Common";
import { Results } from "./Results";
import { Variables } from "./Variables";
import { SourceFiles } from "./SourceFiles";
import { TableGroup } from "./forms/Groups";
import { Helpers } from "./Helpers";
import { InfoGrid } from "./InfoGrid";
import { Queries } from "./Queries";
import { Resources } from "./Resources";
import { WUXMLSourceEditor } from "./SourceEditor";
import { Workflows } from "./Workflows";

import "react-reflex/styles.css";

const classNames = mergeStyleSets({
    reflexScrollPane: {
        borderWidth: 1,
        borderStyle: "solid",
        borderColor: "darkgray"
    },
    reflexPane: {
        borderWidth: 1,
        borderStyle: "solid",
        borderColor: "darkgray",
        overflow: "hidden"
    },
    reflexSplitter: {
        position: "relative",
        height: "5px",
        backgroundColor: "transparent",
        borderStyle: "none"
    },
    reflexSplitterDiv: {
        fontFamily: "Lucida Sans,Lucida Grande,Arial !important",
        fontSize: "13px !important",
        cursor: "row-resize",
        position: "absolute",
        left: "49%",
        background: "#9e9e9e",
        height: "1px",
        top: "2px",
        width: "19px"
    }
});

const pivotItemStyle = (size, padding: number = 4) => {
    if (isNaN(size.width)) {
        return { position: "absolute", padding: `${padding}px`, overflow: "auto", zIndex: 0 } as React.CSSProperties;
    }
    return { position: "absolute", padding: `${padding}px`, overflow: "auto", zIndex: 0, width: size.width - padding * 2, height: size.height - 45 - padding * 2 } as React.CSSProperties;
};

interface WorkunitDetailsProps {
    wuid: string;
    tab?: string;
}

export const WorkunitDetails: React.FunctionComponent<WorkunitDetailsProps> = ({
    wuid,
    tab = "summary"
}) => {

    const [workunit] = useWorkunit(wuid, true);
    const [jobname, setJobname] = React.useState("");
    const [description, setDescription] = React.useState("");
    const [_protected, setProtected] = React.useState(false);

    React.useEffect(() => {
        setJobname(jobname || workunit?.Jobname);
        setDescription(description || workunit?.Description);
        setProtected(_protected || workunit?.Protected);

        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [workunit?.Jobname, workunit?.Jobname, workunit?.Jobname]);

    const canSave = workunit && (
        jobname !== workunit.Jobname ||
        description !== workunit.Description ||
        _protected !== workunit.Protected
    );

    const buttons: ICommandBarItemProps[] = [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => {
                workunit.refresh();
            }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "save", text: nlsHPCC.Save, iconProps: { iconName: "Save" }, disabled: !canSave,
            onClick: () => {
                workunit?.update({
                    Jobname: jobname,
                    Description: description,
                    Protected: _protected
                });
            }
        },
        {
            key: "copy", text: nlsHPCC.CopyWUID, iconProps: { iconName: "Copy" },
            onClick: () => {
                navigator?.clipboard?.writeText(wuid);
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
    ];

    const protectedImage = getImageURL(workunit?.Protected ? "locked.png" : "unlocked.png");
    const stateIconClass = getStateIconClass(workunit?.StateID, workunit?.isComplete(), workunit?.Archived);
    const serviceNames = workunit?.ServiceNames?.Item?.join("\n") || "";
    const resourceCount = workunit?.ResourceURLCount > 1 ? workunit?.ResourceURLCount - 1 : undefined;

    return <SizeMe monitorHeight>{({ size }) =>
        <Pivot overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab} onLinkClick={evt => pushUrl(`/workunits/${wuid}/${evt.props.itemKey}`)}>
            <PivotItem headerText={wuid} itemKey="summary" style={pivotItemStyle(size)}>
                <div style={{ height: "100%", position: "relative" }}>
                    <ReflexContainer orientation="horizontal">
                        <ReflexElement className={classNames.reflexScrollPane}>
                            <div className="pane-content">
                                <ScrollablePane scrollbarVisibility={ScrollbarVisibility.auto}>
                                    <Sticky stickyPosition={StickyPositionType.Header}>
                                        <CommandBar items={buttons} />
                                    </Sticky>
                                    <Sticky stickyPosition={StickyPositionType.Header}>
                                        <div style={{ display: "inline-block" }}>
                                            <h2>
                                                <img src={protectedImage} />&nbsp;<div className={stateIconClass}></div>&nbsp;<span className="bold">{wuid}</span>
                                            </h2>
                                        </div>
                                        <div style={{ width: "512px", height: "64px", float: "right" }}>
                                            <WUStatus wuid={wuid}></WUStatus>
                                        </div>
                                    </Sticky>
                                    <TableGroup fields={{
                                        "wuid": { label: nlsHPCC.WUID, type: "string", value: wuid, readonly: true },
                                        "action": { label: nlsHPCC.Action, type: "string", value: workunit?.ActionEx, readonly: true },
                                        "state": { label: nlsHPCC.State, type: "string", value: workunit?.State, readonly: true },
                                        "owner": { label: nlsHPCC.Owner, type: "string", value: workunit?.Owner, readonly: true },
                                        "jobname": { label: nlsHPCC.JobName, type: "string", value: jobname },
                                        "description": { label: nlsHPCC.Description, type: "string", value: description },
                                        "protected": { label: nlsHPCC.Protected, type: "checkbox", value: _protected },
                                        "cluster": { label: nlsHPCC.Cluster, type: "string", value: workunit?.Cluster, readonly: true },
                                        "totalClusterTime": { label: nlsHPCC.TotalClusterTime, type: "string", value: workunit?.TotalClusterTime, readonly: true },
                                        "abortedBy": { label: nlsHPCC.AbortedBy, type: "string", value: workunit?.AbortBy, readonly: true },
                                        "abortedTime": { label: nlsHPCC.AbortedTime, type: "string", value: workunit?.AbortTime, readonly: true },
                                        "ServiceNamesCustom": { label: nlsHPCC.Services, type: "string", value: serviceNames, readonly: true, multiline: true },
                                    }} onChange={(id, value) => {
                                        switch (id) {
                                            case "jobname":
                                                setJobname(value);
                                                break;
                                            case "description":
                                                setDescription(value);
                                                break;
                                            case "protected":
                                                setProtected(value);
                                                break;
                                            default:
                                                console.log(id, value);
                                        }
                                    }} />
                                </ScrollablePane>
                            </div>
                        </ReflexElement>
                        <ReflexSplitter style={{ position: "relative", height: "5px", backgroundColor: "transparent", borderStyle: "none" }}>
                            <div className={classNames.reflexSplitterDiv}></div>
                        </ReflexSplitter>
                        <ReflexElement propagateDimensions={true} className={classNames.reflexPane} style={{ overflow: "hidden" }}>
                            <InfoGrid wuid={wuid}></InfoGrid>
                        </ReflexElement>
                    </ReflexContainer>
                </div>
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Variables} itemCount={(workunit?.VariableCount || 0) + (workunit?.ApplicationValueCount || 0) + (workunit?.DebugValueCount || 0)} itemKey="variables" style={pivotItemStyle(size, 0)}>
                <Variables wuid={wuid} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Outputs} itemKey="outputs" itemCount={workunit?.ResultCount} style={pivotItemStyle(size, 0)}>
                <Results wuid={wuid} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Inputs} itemKey="inputs" itemCount={workunit?.SourceFileCount} style={pivotItemStyle(size, 0)}>
                <SourceFiles wuid={wuid} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Timers} itemKey="timers" itemCount={workunit?.TimerCount} style={pivotItemStyle(size, 0)}>
                <DojoAdapter widgetClassID="TimingPageWidget" params={{ Wuid: wuid }} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Graphs} itemKey="graphs" itemCount={workunit?.GraphCount} style={pivotItemStyle(size, 0)}>
                <DojoAdapter widgetClassID="GraphsWUWidget" params={{ Wuid: wuid }} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Workflows} itemKey="workflows" itemCount={workunit?.WorkflowCount} style={pivotItemStyle(size, 0)}>
                <Workflows wuid={wuid} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Queries} itemIcon="Search" itemKey="queries" style={pivotItemStyle(size, 0)}>
                <Queries wuid={wuid} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Resources} itemKey="resources" itemCount={resourceCount} style={pivotItemStyle(size, 0)}>
                <Resources wuid={wuid} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Helpers} itemKey="helpers" itemCount={workunit?.HelpersCount} style={pivotItemStyle(size, 0)}>
                <Helpers wuid={wuid} />
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
