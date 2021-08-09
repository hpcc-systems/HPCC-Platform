import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Pivot, PivotItem, ScrollablePane, ScrollbarVisibility, Sticky, StickyPositionType } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { WUStatus } from "src/react/index";
import { useWorkunit } from "../hooks/workunit";
import { useFavorite } from "../hooks/favorite";
import { DojoAdapter } from "../layouts/DojoAdapter";
import { pivotItemStyle } from "../layouts/pivot";
import { ReflexContainer, ReflexElement, ReflexSplitter, classNames, styles } from "../layouts/react-reflex";
import { pushUrl } from "../util/history";
import { ShortVerticalDivider } from "./Common";
import { Results } from "./Results";
import { Variables } from "./Variables";
import { SourceFiles } from "./SourceFiles";
import { TableGroup } from "./forms/Groups";
import { PublishQueryForm } from "./forms/PublishQuery";
import { SlaveLogs } from "./forms/SlaveLogs";
import { ZAPDialog } from "./forms/ZAPDialog";
import { Helpers } from "./Helpers";
import { InfoGrid } from "./InfoGrid";
import { Queries } from "./Queries";
import { Resources } from "./Resources";
import { WUXMLSourceEditor } from "./SourceEditor";
import { Workflows } from "./Workflows";
import { Metrics } from "./Metrics";
import { WorkunitPersona } from "./controls/StateIcon";

const logger = scopedLogger("../components/WorkunitDetails.tsx");

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
    const [isFavorite, addFavorite, removeFavorite] = useFavorite(window.location.hash);
    const [showPublishForm, setShowPublishForm] = React.useState(false);
    const [showZapForm, setShowZapForm] = React.useState(false);
    const [showThorSlaveLogs, setShowThorSlaveLogs] = React.useState(false);

    React.useEffect(() => {
        setJobname(workunit?.Jobname);
        setDescription(workunit?.Description);
        setProtected(workunit?.Protected);
    }, [workunit?.Description, workunit?.Jobname, workunit?.Protected]);

    const canSave = workunit && (
        jobname !== workunit.Jobname ||
        description !== workunit.Description ||
        _protected !== workunit.Protected
    );
    const canDelete = workunit && (
        _protected !== workunit.Protected ||
        999 !== workunit.StateID ||
        workunit.Archived
    );
    const canDeschedule = workunit && workunit?.EventSchedule === 2;
    const canReschedule = workunit && workunit?.EventSchedule === 1;

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => {
                workunit.refresh();
            }
        },
        {
            key: "copy", text: nlsHPCC.CopyWUID, iconProps: { iconName: "Copy" },
            onClick: () => {
                navigator?.clipboard?.writeText(wuid);
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
                }).catch(logger.error);
            }
        },
        {
            key: "delete", text: nlsHPCC.Delete, iconProps: { iconName: "Delete" }, disabled: !canDelete,
            onClick: () => {
                if (confirm(nlsHPCC.YouAreAboutToDeleteThisWorkunit)) {
                    workunit?.delete().catch(logger.error);
                    pushUrl("/workunits");
                }
            }
        },
        {
            key: "restore", text: nlsHPCC.Restore, disabled: !workunit?.Archived,
            onClick: () => workunit?.restore().catch(logger.error)

        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "reschedule", text: nlsHPCC.Reschedule, disabled: !canReschedule,
            onClick: () => workunit?.reschedule().catch(logger.error)
        },
        {
            key: "deschedule", text: nlsHPCC.Deschedule, disabled: !canDeschedule,
            onClick: () => workunit?.deschedule().catch(logger.error)
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "setToFailed", text: nlsHPCC.SetToFailed, disabled: workunit?.Archived || workunit?.isComplete() || workunit?.isDeleted(),
            onClick: () => workunit?.setToFailed().catch(logger.error)
        },
        {
            key: "abort", text: nlsHPCC.Abort, disabled: workunit?.Archived || workunit?.isComplete() || workunit?.isDeleted(),
            onClick: () => workunit?.abort().catch(logger.error)
        },
        { key: "divider_4", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "recover", text: nlsHPCC.Recover, disabled: workunit?.Archived || !workunit?.isComplete() || workunit?.isDeleted(),
            onClick: () => workunit?.resubmit().catch(logger.error)
        },
        {
            key: "resubmit", text: nlsHPCC.Resubmit, disabled: workunit?.Archived || !workunit?.isComplete() || workunit?.isDeleted(),
            onClick: () => workunit?.resubmit().catch(logger.error)
        },
        {
            key: "clone", text: nlsHPCC.Clone, disabled: workunit?.Archived || !workunit?.isComplete() || workunit?.isDeleted(),
            onClick: () => {
                workunit?.clone().then(wu => {
                    if (wu && wu.Wuid) {
                        pushUrl(`/workunits/${wu?.Wuid}`);
                    }
                }).catch(logger.error);
            }
        },
        { key: "divider_5", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "publish", text: nlsHPCC.Publish,
            onClick: () => setShowPublishForm(true)
        },
        { key: "divider_6", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "zap", text: nlsHPCC.ZAP, disabled: !canDelete,
            onClick: () => setShowZapForm(true)
        },
        { key: "divider_7", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "slaveLogs", text: nlsHPCC.SlaveLogs, disabled: !workunit?.ThorLogList,
            onClick: () => setShowThorSlaveLogs(true)
        },
    ], [_protected, canDelete, canDeschedule, canReschedule, canSave, description, jobname, workunit, wuid]);

    const rightButtons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "star", iconProps: { iconName: isFavorite ? "FavoriteStarFill" : "FavoriteStar" },
            onClick: () => {
                if (isFavorite) {
                    removeFavorite();
                } else {
                    addFavorite();
                }
            }
        }
    ], [addFavorite, isFavorite, removeFavorite]);

    const serviceNames = workunit?.ServiceNames?.Item?.join("\n") || "";
    const resourceCount = workunit?.ResourceURLCount > 1 ? workunit?.ResourceURLCount - 1 : undefined;

    return <>
        <SizeMe monitorHeight>{({ size }) =>
            <Pivot overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab} onLinkClick={evt => pushUrl(`/workunits/${wuid}/${evt.props.itemKey}`)}>
                <PivotItem headerText={wuid} itemKey="summary" style={pivotItemStyle(size)} >
                    <div style={{ height: "100%", position: "relative" }}>
                        <ReflexContainer orientation="horizontal">
                            <ReflexElement className={classNames.reflexScrollPane}>
                                <div className="pane-content">
                                    <ScrollablePane scrollbarVisibility={ScrollbarVisibility.auto}>
                                        <Sticky stickyPosition={StickyPositionType.Header}>
                                            <CommandBar items={buttons} farItems={rightButtons} />
                                        </Sticky>
                                        <Sticky stickyPosition={StickyPositionType.Header}>
                                            <WorkunitPersona wuid={wuid} />
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
                                                    logger.debug(`${id}:  ${value}`);
                                            }
                                        }} />
                                    </ScrollablePane>
                                </div>
                            </ReflexElement>
                            <ReflexSplitter style={styles.reflexSplitter}>
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
                <PivotItem headerText={nlsHPCC.Metrics} itemKey="metrics" style={pivotItemStyle(size, 0)}>
                    <Metrics wuid={wuid} filter={{}} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.Timers + " (L)"} itemKey="timers" itemCount={workunit?.TimerCount} style={pivotItemStyle(size, 0)}>
                    <DojoAdapter widgetClassID="TimingPageWidget" params={{ Wuid: wuid }} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.Graphs + " (L)"} itemKey="graphs" itemCount={workunit?.GraphCount} style={pivotItemStyle(size, 0)}>
                    <DojoAdapter widgetClassID="GraphsWUWidget" params={{ Wuid: wuid }} />
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
                    <Helpers wuid={wuid} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.ECL} itemKey="eclsummary" style={pivotItemStyle(size, 0)}>
                    <DojoAdapter widgetClassID="ECLArchiveWidget" params={{ Wuid: wuid }} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.XML} itemKey="xml" style={pivotItemStyle(size, 0)}>
                    <WUXMLSourceEditor wuid={wuid} />
                </PivotItem>
            </Pivot>
        }</SizeMe>
        <PublishQueryForm wuid={wuid} showForm={showPublishForm} setShowForm={setShowPublishForm} />
        <ZAPDialog wuid={wuid} showForm={showZapForm} setShowForm={setShowZapForm} />
        <SlaveLogs wuid={wuid} showForm={showThorSlaveLogs} setShowForm={setShowThorSlaveLogs} />
    </>;
};
