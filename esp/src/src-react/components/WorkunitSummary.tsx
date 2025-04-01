import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, mergeStyles, MessageBar, MessageBarType, registerIcons, ScrollablePane, ScrollbarVisibility, Sticky, StickyPositionType } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { WUStatus } from "src/react/index";
import { formatCost } from "src/Session";
import { isNumeric } from "src/Utility";
import { useConfirm } from "../hooks/confirm";
import { useWorkunit, useWorkunitExceptions } from "../hooks/workunit";
import { ReflexContainer, ReflexElement, ReflexSplitter } from "../layouts/react-reflex";
import { pushUrl, replaceUrl } from "../util/history";
import { HolyGrail } from "../layouts/HolyGrail";
import { ShortVerticalDivider } from "./Common";
import { TableGroup } from "./forms/Groups";
import { PublishQueryForm } from "./forms/PublishQuery";
import { SlaveLogs } from "./forms/SlaveLogs";
import { ZAPDialog } from "./forms/ZAPDialog";
import { InfoGrid } from "./InfoGrid";
import { WorkunitPersona } from "./controls/StateIcon";

const logger = scopedLogger("../components/WorkunitDetails.tsx");

registerIcons({
    icons: {
        "open-telemetry": (
            // .../eclwatch/img/opentelemetry-icon-color.svg
            <svg xmlns="http://www.w3.org/2000/svg" role="img" viewBox="-12.70 -12.70 1024.40 1024.40"><path fill="#f5a800" d="M528.7 545.9c-42 42-42 110.1 0 152.1s110.1 42 152.1 0 42-110.1 0-152.1-110.1-42-152.1 0zm113.7 113.8c-20.8 20.8-54.5 20.8-75.3 0-20.8-20.8-20.8-54.5 0-75.3 20.8-20.8 54.5-20.8 75.3 0 20.8 20.7 20.8 54.5 0 75.3zm36.6-643l-65.9 65.9c-12.9 12.9-12.9 34.1 0 47l257.3 257.3c12.9 12.9 34.1 12.9 47 0l65.9-65.9c12.9-12.9 12.9-34.1 0-47L725.9 16.7c-12.9-12.9-34-12.9-46.9 0zM217.3 858.8c11.7-11.7 11.7-30.8 0-42.5l-33.5-33.5c-11.7-11.7-30.8-11.7-42.5 0L72.1 852l-.1.1-19-19c-10.5-10.5-27.6-10.5-38 0-10.5 10.5-10.5 27.6 0 38l114 114c10.5 10.5 27.6 10.5 38 0s10.5-27.6 0-38l-19-19 .1-.1 69.2-69.2z" /><path fill="#425cc7" d="M565.9 205.9L419.5 352.3c-13 13-13 34.4 0 47.4l90.4 90.4c63.9-46 153.5-40.3 211 17.2l73.2-73.2c13-13 13-34.4 0-47.4L613.3 205.9c-13-13.1-34.4-13.1-47.4 0zm-94 322.3l-53.4-53.4c-12.5-12.5-33-12.5-45.5 0L184.7 663.2c-12.5 12.5-12.5 33 0 45.5l106.7 106.7c12.5 12.5 33 12.5 45.5 0L458 694.1c-25.6-52.9-21-116.8 13.9-165.9z" /></svg>
        ),
        "open-telemetry-disabled": (
            <svg xmlns="http://www.w3.org/2000/svg" role="img" viewBox="-12.70 -12.70 1024.40 1024.40"><path fill="var(--colorNeutralForegroundDisabled)" d="M528.7 545.9c-42 42-42 110.1 0 152.1s110.1 42 152.1 0 42-110.1 0-152.1-110.1-42-152.1 0zm113.7 113.8c-20.8 20.8-54.5 20.8-75.3 0-20.8-20.8-20.8-54.5 0-75.3 20.8-20.8 54.5-20.8 75.3 0 20.8 20.7 20.8 54.5 0 75.3zm36.6-643l-65.9 65.9c-12.9 12.9-12.9 34.1 0 47l257.3 257.3c12.9 12.9 34.1 12.9 47 0l65.9-65.9c12.9-12.9 12.9-34.1 0-47L725.9 16.7c-12.9-12.9-34-12.9-46.9 0zM217.3 858.8c11.7-11.7 11.7-30.8 0-42.5l-33.5-33.5c-11.7-11.7-30.8-11.7-42.5 0L72.1 852l-.1.1-19-19c-10.5-10.5-27.6-10.5-38 0-10.5 10.5-10.5 27.6 0 38l114 114c10.5 10.5 27.6 10.5 38 0s10.5-27.6 0-38l-19-19 .1-.1 69.2-69.2z" /><path fill="var(--colorNeutralForegroundDisabled)" d="M565.9 205.9L419.5 352.3c-13 13-13 34.4 0 47.4l90.4 90.4c63.9-46 153.5-40.3 211 17.2l73.2-73.2c13-13 13-34.4 0-47.4L613.3 205.9c-13-13.1-34.4-13.1-47.4 0zm-94 322.3l-53.4-53.4c-12.5-12.5-33-12.5-45.5 0L184.7 663.2c-12.5 12.5-12.5 33 0 45.5l106.7 106.7c12.5 12.5 33 12.5 45.5 0L458 694.1c-25.6-52.9-21-116.8 13.9-165.9z" /></svg>
        )
    }
});

const otIconStyle = mergeStyles({
    width: 16
});

interface OtTraceSchema {
    traceId: string;
    spanId: string;
}

const parseOtTraceParent = (parent: string = ""): OtTraceSchema => {
    const retVal = { traceId: "", spanId: "" };
    const regex = /00\-([0-9a-z]+)\-([0-9a-z]+)\-01/;
    const matches = parent.match(regex);
    if (matches) {
        retVal.traceId = matches[1] ?? "";
        retVal.spanId = matches[2] ?? "";
    }
    return retVal;
};

interface MessageBarContent {
    type: MessageBarType;
    message: string;
}

interface WorkunitSummaryProps {
    wuid: string;
    otTraceParent?: string;
}

export const WorkunitSummary: React.FunctionComponent<WorkunitSummaryProps> = ({
    wuid,
    otTraceParent = ""
}) => {

    const [workunit, , , , refresh] = useWorkunit(wuid, true);
    const [exceptions, , refreshSavings] = useWorkunitExceptions(wuid);
    const [jobname, setJobname] = React.useState("");
    const [description, setDescription] = React.useState("");
    const [otTraceId, setOtTraceId] = React.useState("");
    const [otSpanId, setOtSpanId] = React.useState("");
    const [wuProtected, setWuProtected] = React.useState(false);
    const [showPublishForm, setShowPublishForm] = React.useState(false);
    const [showZapForm, setShowZapForm] = React.useState(false);
    const [showThorSlaveLogs, setShowThorSlaveLogs] = React.useState(false);

    const [messageBarContent, setMessageBarContent] = React.useState<MessageBarContent | undefined>();
    const dismissMessageBar = React.useCallback(() => setMessageBarContent(undefined), []);
    const showMessageBar = React.useCallback((content: MessageBarContent) => {
        setMessageBarContent(content);
        const t = window.setTimeout(function () {
            dismissMessageBar();
            window.clearTimeout(t);
        }, 2400);
    }, [dismissMessageBar]);

    React.useEffect(() => {
        setJobname(workunit?.Jobname);
        setDescription(workunit?.Description);
        setWuProtected(workunit?.Protected);
    }, [workunit?.Description, workunit?.Jobname, workunit?.Protected]);

    React.useEffect(() => {
        const otTrace = parseOtTraceParent(otTraceParent);
        setOtTraceId(otTrace.traceId);
        setOtSpanId(otTrace.spanId);
    }, [otTraceParent]);

    const canSave = workunit && (
        jobname !== workunit.Jobname ||
        description !== workunit.Description ||
        wuProtected !== workunit.Protected
    );
    const canDelete = React.useMemo(() => {
        return (wuProtected !== workunit?.Protected || 999 !== workunit?.StateID) && workunit?.Archived === false;
    }, [workunit?.Archived, workunit?.Protected, workunit?.StateID, wuProtected]);

    const canDeschedule = workunit && workunit?.EventSchedule === 2;
    const canReschedule = workunit && workunit?.EventSchedule === 1;

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.YouAreAboutToDeleteThisWorkunit,
        onSubmit: React.useCallback(() => {
            workunit?.delete()
                .then(response => replaceUrl("/workunits"))
                .catch(err => logger.error(err))
                ;
        }, [workunit])
    });

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => {
                refresh(true);
                refreshSavings();
            }
        },
        {
            key: "copy", text: nlsHPCC.CopyWUID, iconProps: { iconName: "Copy" },
            onClick: () => {
                navigator?.clipboard?.writeText(wuid);
            }
        },
        {
            key: "copyOtel", text: nlsHPCC.CopyOpenTelemetry, iconProps: { iconName: otTraceParent === "" ? "open-telemetry-disabled" : "open-telemetry", className: otIconStyle },
            disabled: otTraceParent === "",
            onClick: () => {
                navigator?.clipboard?.writeText(JSON.stringify(parseOtTraceParent(otTraceParent)));
            }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "save", text: nlsHPCC.Save, iconProps: { iconName: "Save" }, disabled: !canSave,
            onClick: () => {
                workunit?.update({
                    Jobname: jobname,
                    Description: description,
                    Protected: wuProtected
                }).then(_ => {
                    showMessageBar({ type: MessageBarType.success, message: nlsHPCC.SuccessfullySaved });
                }).catch(err => logger.error(err));
            }
        },
        {
            key: "delete", text: nlsHPCC.Delete, iconProps: { iconName: "Delete" }, disabled: !canDelete,
            onClick: () => setShowDeleteConfirm(true)
        },
        {
            key: "restore", text: nlsHPCC.Restore, disabled: !workunit?.Archived,
            onClick: () => workunit?.restore().catch(err => logger.error(err))

        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "reschedule", text: nlsHPCC.Reschedule, disabled: !canReschedule,
            onClick: () => workunit?.reschedule().catch(err => logger.error(err))
        },
        {
            key: "deschedule", text: nlsHPCC.Deschedule, disabled: !canDeschedule,
            onClick: () => workunit?.deschedule().catch(err => logger.error(err))
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "setToFailed", text: nlsHPCC.SetToFailed, disabled: workunit?.Archived || workunit?.isComplete() || workunit?.isDeleted(),
            onClick: () => workunit?.setToFailed().catch(err => logger.error(err))
        },
        {
            key: "abort", text: nlsHPCC.Abort, disabled: workunit?.Archived || workunit?.isComplete() || workunit?.isDeleted(),
            onClick: () => workunit?.abort().catch(err => logger.error(err))
        },
        { key: "divider_4", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "recover", text: nlsHPCC.Recover, disabled: workunit?.Archived || !workunit?.isComplete() || workunit?.isDeleted(),
            onClick: () => workunit?.resubmit().catch(err => logger.error(err))
        },
        {
            key: "resubmit", text: nlsHPCC.Resubmit, disabled: workunit?.Archived || !workunit?.isComplete() || workunit?.isDeleted(),
            onClick: () => workunit?.resubmit().catch(err => logger.error(err))
        },
        {
            key: "clone", text: nlsHPCC.Clone, disabled: workunit?.Archived || !workunit?.isComplete() || workunit?.isDeleted(),
            onClick: () => {
                workunit?.clone().then(wu => {
                    if (wu && wu.Wuid) {
                        pushUrl(`/workunits/${wu?.Wuid}`);
                    }
                }).catch(err => logger.error(err));
            }
        },
        { key: "divider_5", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "publish", text: nlsHPCC.Publish, disabled: !canDelete,
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
    ], [wuProtected, canDelete, canDeschedule, canReschedule, canSave, description, jobname, otTraceParent, refresh, refreshSavings, setShowDeleteConfirm, showMessageBar, workunit, wuid]);

    const serviceNames = React.useMemo(() => {
        return workunit?.ServiceNames?.Item?.join("\n") || "";
    }, [workunit?.ServiceNames?.Item]);

    const totalCosts = React.useMemo(() => {
        return (workunit?.CompileCost ?? 0) +
            (workunit?.ExecuteCost ?? 0) +
            (workunit?.FileAccessCost ?? 0);
    }, [workunit?.CompileCost, workunit?.ExecuteCost, workunit?.FileAccessCost]);

    const potentialSavings = React.useMemo(() => {
        return exceptions.reduce((prev, cur) => {
            if (isNumeric(cur.Cost)) {
                prev += cur.Cost;
            }
            return prev;
        }, 0) || 0;
    }, [exceptions]);

    return <HolyGrail
        main={<>
            <ReflexContainer orientation="horizontal">
                <ReflexElement>
                    <div className="pane-content">
                        <ScrollablePane scrollbarVisibility={ScrollbarVisibility.auto}>
                            <Sticky stickyPosition={StickyPositionType.Header}>
                                <CommandBar items={buttons} />
                                {messageBarContent &&
                                    <MessageBar messageBarType={messageBarContent.type} dismissButtonAriaLabel={nlsHPCC.Close} onDismiss={dismissMessageBar} >
                                        {messageBarContent.message}
                                    </MessageBar>
                                }
                            </Sticky>
                            <div style={{ position: "sticky", zIndex: 2, top: 44, display: "flex", flexDirection: "row", justifyContent: "space-between" }}>
                                <WorkunitPersona wuid={wuid} />
                                <WUStatus wuid={wuid}></WUStatus>
                            </div>
                            <TableGroup fields={{
                                "wuid": { label: nlsHPCC.WUID, type: "string", value: wuid, readonly: true },
                                "otTraceId": { label: nlsHPCC.Trace, type: "string", value: otTraceId, readonly: true },
                                "otSpanId": { label: nlsHPCC.Span, type: "string", value: otSpanId, readonly: true },
                                "action": { label: nlsHPCC.Action, type: "string", value: workunit?.ActionEx, readonly: true },
                                "state": { label: nlsHPCC.State, type: "string", value: workunit?.State + (workunit?.StateEx ? ` (${workunit.StateEx})` : ""), readonly: true },
                                "owner": { label: nlsHPCC.Owner, type: "string", value: workunit?.Owner, readonly: true },
                                "jobname": { label: nlsHPCC.JobName, type: "string", value: jobname },
                                "description": { label: nlsHPCC.Description, type: "string", value: description },
                                "potentialSavings": { label: nlsHPCC.PotentialSavings, type: "string", value: `${formatCost(potentialSavings)} (${totalCosts > 0 ? Math.round((potentialSavings / totalCosts) * 10000) / 100 : 0}%)`, readonly: true },
                                "compileCost": { label: nlsHPCC.CompileCost, type: "string", value: `${formatCost(workunit?.CompileCost)}`, readonly: true },
                                "executeCost": { label: nlsHPCC.ExecuteCost, type: "string", value: `${formatCost(workunit?.ExecuteCost)}`, readonly: true },
                                "fileAccessCost": { label: nlsHPCC.FileAccessCost, type: "string", value: `${formatCost(workunit?.FileAccessCost)}`, readonly: true },
                                "protected": { label: nlsHPCC.Protected, type: "checkbox", value: wuProtected },
                                "cluster": { label: nlsHPCC.Cluster, type: "string", value: workunit?.Cluster, readonly: true },
                                "totalClusterTime": { label: nlsHPCC.TotalClusterTime, type: "string", value: workunit?.TotalClusterTime ? workunit?.TotalClusterTime : "0.00", readonly: true },
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
                                        setWuProtected(value);
                                        break;
                                    default:
                                        logger.debug(`${id}:  ${value}`);
                                }
                            }} />
                        </ScrollablePane>
                    </div>
                </ReflexElement>
                <ReflexSplitter />
                <ReflexElement>
                    <InfoGrid wuid={wuid}></InfoGrid>
                </ReflexElement>
            </ReflexContainer>
            <PublishQueryForm wuid={wuid} showForm={showPublishForm} setShowForm={setShowPublishForm} />
            <ZAPDialog wuid={wuid} showForm={showZapForm} setShowForm={setShowZapForm} />
            <SlaveLogs wuid={wuid} showForm={showThorSlaveLogs} setShowForm={setShowThorSlaveLogs} />
            <DeleteConfirm />
        </>}
    />;
};
