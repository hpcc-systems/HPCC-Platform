import * as React from "react";
import { ScrollablePane, ScrollbarVisibility } from "./controls/ScrollablePane";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "./CommandBarV9";
import { Button, Card, Link, makeStyles, MessageBar, MessageBarActions, MessageBarBody, MessageBarIntent, tokens } from "@fluentui/react-components";
import { DismissRegular, PersonRegular } from "@fluentui/react-icons";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { WUStatus } from "src/react/index";
import { formatCost } from "src/Session";
import { isNumeric } from "src/Utility";
import { useConfirm } from "../hooks/confirm";
import { useWorkunit, useWorkunitExceptions } from "../hooks/workunit";
import { useLocalStore } from "../hooks/store";
import { pushUrl, replaceUrl } from "../util/history";
import { HolyGrail } from "../layouts/HolyGrail";
import { DockPanel, DockPanelItem, ResetableDockPanel } from "../layouts/DockPanel";
import { TableGroup } from "./forms/Groups";
import { PublishQueryForm } from "./forms/PublishQuery";
import { SlaveLogs } from "./forms/SlaveLogs";
import { ZAPDialog } from "./forms/ZAPDialog";
import { InfoGrid } from "./InfoGrid";
import { WorkunitPersona } from "./controls/StateIcon";
import { localKeyValStore } from "src/KeyValStore";
import { copyToClipboard } from "src/Utility";

const logger = scopedLogger("../components/WorkunitSummary.tsx");

const WU_SUMMARY_SPLITTER = "workunit_summary_splitter";

export function resetWorkunitSummarySplitter() {
    const store = localKeyValStore();
    return store?.delete(WU_SUMMARY_SPLITTER);
}

interface OtTraceSchema {
    traceId: string;
    spanId: string;
}

const parseOtTraceParent = (parent: string = ""): OtTraceSchema => {
    const retVal = { traceId: "", spanId: "" };
    const regex = /[0-9a-f]{2}\-([0-9a-f]{32})\-([0-9a-f]{16})\-[0-9a-f]{2}/;
    const matches = parent.trim().match(regex);
    if (matches) {
        retVal.traceId = matches[1] ?? "";
        retVal.spanId = matches[2] ?? "";
    }
    return retVal;
};

const useStyles = makeStyles({
    wuSummaryHeader: {
        position: "sticky",
        top: 0,
        marginBottom: "8px",
        background: tokens.colorNeutralBackground1,
        borderBottom: `1px solid ${tokens.colorNeutralBackground1Pressed}`,
        zIndex: 2,
        display: "flex",
        flexDirection: "row",
        flexWrap: "wrap",
        alignItems: "center",
        containerType: "inline-size"
    },
    wuPersona: {
        marginTop: "-6px",
        "@container (max-width: 1020px)": {
            marginTop: "0"
        },
        "@container (max-width: 900px)": {
            flexBasis: "100%"
        }
    },
    linkWrapper: {
        margin: "-6px 10px 0 14px",
        "@container (max-width: 1020px)": {
            marginTop: "0"
        }
    },
    linkSeparator: {
        color: tokens.colorNeutralForeground3,
        margin: "0 6px"
    },
    jobNameLink: {
        alignItems: "center",
        display: "inline-flex",
        gap: "4px",
        lineHeight: "20px",
        verticalAlign: "middle"
    },
    ownerIcon: {
        fontSize: "16px",
        lineHeight: 1,
        transform: "translateY(1px)"
    },
    ownerLink: {
        alignItems: "center",
        display: "inline-flex",
        gap: "4px",
        lineHeight: "20px",
        verticalAlign: "middle"
    },
    cardsWrapper: {
        display: "flex",
        gap: "12px",
        margin: "0 4px 4px 4px",
        alignItems: "flex-start",
        containerType: "inline-size",
        flexWrap: "wrap"

    },
    detailsPanel: {
        alignSelf: "flex-start",
        flexGrow: 1,
        overflowX: "auto",
        "@container (max-width: 700px)": {
            maxWidth: "100%",
            flexBasis: "100%"
        }
    },
    costsCard: {
        alignSelf: "flex-start",
        marginLeft: "auto",
        overflow: "visible",
        "@container (max-width: 700px)": {
            width: "100%",
            flexBasis: "100%",
            marginLeft: "0"
        }
    }
});

interface MessageBarContent {
    type: MessageBarIntent;
    message: string;
}

interface WorkunitSummaryProps {
    wuid: string;
    otTraceParent?: string;
    engineRedirected?: boolean;
    targetClusterType?: string;
}

export const WorkunitSummary: React.FunctionComponent<WorkunitSummaryProps> = ({
    wuid,
    otTraceParent = "",
    engineRedirected = false,
    targetClusterType = ""
}) => {
    const { workunit, lastUpdate, refresh } = useWorkunit(wuid, true);
    const [exceptions, , refreshSavings] = useWorkunitExceptions(wuid);
    const [jobname, setJobname] = React.useState("");
    const [description, setDescription] = React.useState("");
    const userEditedRef = React.useRef(false);
    const [otTraceId, setOtTraceId] = React.useState("");
    const [otSpanId, setOtSpanId] = React.useState("");
    const [wuProtected, setWuProtected] = React.useState(false);
    const [showPublishForm, setShowPublishForm] = React.useState(false);
    const [showZapForm, setShowZapForm] = React.useState(false);
    const [showThorSlaveLogs, setShowThorSlaveLogs] = React.useState(false);
    const [dockpanel, setDockpanel] = React.useState<ResetableDockPanel>();
    const [layout, setLayout] = useLocalStore<[number, number]>(WU_SUMMARY_SPLITTER, [0.67, 0.33], false);

    const styles = useStyles();

    const [messageBarContent, setMessageBarContent] = React.useState<MessageBarContent | undefined>();
    const dismissMessageBar = React.useCallback(() => setMessageBarContent(undefined), []);
    const showMessageBar = React.useCallback((content: MessageBarContent) => {
        setMessageBarContent(content);
        const t = window.setTimeout(function () {
            dismissMessageBar();
            window.clearTimeout(t);
        }, 2400);
    }, [dismissMessageBar]);

    const [minimized, setMinimized] = React.useState(false);
    const minimizedRef = React.useRef(false);
    const preMinimizeSizes = React.useRef<[number, number] | null>(null);
    // Target pixel height of the minimized InfoGrid (tabBar + commandBar).
    // Stored in pixels so the panel stays at a constant height regardless of
    // window/container resizes. Lumino's min-size constraints may push the
    // settled value slightly higher; we accept that on the first layoutChanged.
    const minimizedPixelHeightRef = React.useRef<number>(0);
    // The container height (dockNode.clientHeight) recorded the last time we
    // applied or accepted a minimized layout. Used to distinguish a container
    // resize (→ re-pin) from a user splitter drag (→ maybe restore).
    const minimizedContainerHeightRef = React.useRef<number>(0);
    const awaitingSettledFractionRef = React.useRef(false);
    // Suppresses the one layoutChanged that fires as a direct result of our
    // own re-pin layout() call, preventing re-entrancy.
    const reapplyingMinimizeRef = React.useRef(false);

    const handleMinimize = React.useCallback((commandBarHeight: number) => {
        if (!dockpanel) return;
        const dpLayout: any = dockpanel.getLayout();
        if (!Array.isArray(dpLayout?.main?.sizes) || dpLayout.main.sizes.length !== 2) return;
        preMinimizeSizes.current = [...dpLayout.main.sizes] as [number, number];
        const dockNode = dockpanel.dockNode();
        const totalHeight = dockNode?.clientHeight ?? 400;
        const tabBarHeight = (dockNode?.querySelector(".lm-TabBar") as HTMLElement)?.offsetHeight ?? 28;
        minimizedPixelHeightRef.current = tabBarHeight + commandBarHeight;
        minimizedContainerHeightRef.current = totalHeight;
        const requestedFraction = minimizedPixelHeightRef.current / totalHeight;
        awaitingSettledFractionRef.current = true;
        dockpanel.layout({ ...dpLayout, main: { ...dpLayout.main, sizes: [1 - requestedFraction, requestedFraction] } }).lazyRender();
        minimizedRef.current = true;
        setMinimized(true);
    }, [dockpanel]);

    //  Only the sizes are restored, so any other layout changes made while minimized are preserved.
    const handleRestore = React.useCallback(() => {
        if (!dockpanel || !preMinimizeSizes.current) return;
        const dpLayout: any = dockpanel.getLayout();
        if (!Array.isArray(dpLayout?.main?.sizes) || dpLayout.main.sizes.length !== 2) return;
        awaitingSettledFractionRef.current = false;
        dockpanel.layout({ ...dpLayout, main: { ...dpLayout.main, sizes: [...preMinimizeSizes.current] } }).lazyRender();
        minimizedRef.current = false;
        setMinimized(false);
    }, [dockpanel]);

    React.useEffect(() => {
        if (!dockpanel) return;
        const origLayoutChanged = dockpanel.layoutChanged.bind(dockpanel);
        dockpanel.layoutChanged = function () {
            origLayoutChanged();
            // Ignore the layoutChanged we triggered ourselves when re-pinning.
            if (reapplyingMinimizeRef.current) {
                reapplyingMinimizeRef.current = false;
                return;
            }
            const dpLayout: any = dockpanel.getLayout();
            if (!Array.isArray(dpLayout?.main?.sizes) || dpLayout.main.sizes.length !== 2) return;
            const totalHeight = dockpanel.dockNode()?.clientHeight ?? 400;
            // First layoutChanged after minimize: lumino's actual settled height
            // may be larger than requested due to min-size constraints. Accept
            // the larger value only when the container is still at the original
            // height (i.e. no resize happened during the settle window).
            if (awaitingSettledFractionRef.current) {
                if (totalHeight === minimizedContainerHeightRef.current) {
                    const settledPixelHeight = dpLayout.main.sizes[1] * totalHeight;
                    if (settledPixelHeight > minimizedPixelHeightRef.current) {
                        minimizedPixelHeightRef.current = settledPixelHeight;
                    }
                }
                minimizedContainerHeightRef.current = totalHeight;
                awaitingSettledFractionRef.current = false;
                return;
            }
            if (!minimizedRef.current) return;
            const targetFraction = minimizedPixelHeightRef.current / totalHeight;
            if (totalHeight !== minimizedContainerHeightRef.current) {
                // The container was resized. Re-pin the fraction so the panel
                // stays at the target pixel height regardless of window size.
                if (Math.abs(dpLayout.main.sizes[1] - targetFraction) > 0.0001) {
                    reapplyingMinimizeRef.current = true;
                    dockpanel.layout({ ...dpLayout, main: { ...dpLayout.main, sizes: [1 - targetFraction, targetFraction] } }).lazyRender();
                }
                minimizedContainerHeightRef.current = totalHeight;
            } else {
                // Container height is unchanged — the user dragged the splitter.
                if (dpLayout.main.sizes[1] > targetFraction * 1.1) {
                    minimizedRef.current = false;
                    setMinimized(false);
                }
            }
        };
        return () => {
            dockpanel.layoutChanged = origLayoutChanged;
        };
    }, [dockpanel]);

    React.useEffect(() => {
        if (!userEditedRef.current) {
            setJobname(workunit?.Jobname);
            setDescription(workunit?.Description);
            setWuProtected(workunit?.Protected);
        }
    }, [workunit?.Description, workunit?.Jobname, workunit?.Protected, lastUpdate]);

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
        { key: "divider_1", itemType: ContextualMenuItemType.Divider },
        {
            key: "save", text: nlsHPCC.Save, iconProps: { iconName: "Save" }, disabled: !canSave,
            onClick: () => {
                workunit?.update({
                    Jobname: jobname,
                    Description: description,
                    Protected: wuProtected
                }).then(_ => {
                    userEditedRef.current = false;
                    showMessageBar({ type: "success", message: nlsHPCC.SuccessfullySaved });
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
        { key: "divider_2", itemType: ContextualMenuItemType.Divider },
        {
            key: "reschedule", text: nlsHPCC.Reschedule, disabled: !canReschedule,
            onClick: () => workunit?.reschedule().catch(err => logger.error(err))
        },
        {
            key: "deschedule", text: nlsHPCC.Deschedule, disabled: !canDeschedule,
            onClick: () => workunit?.deschedule().catch(err => logger.error(err))
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider },
        {
            key: "setToFailed", text: nlsHPCC.SetToFailed, disabled: workunit?.Archived || workunit?.isComplete() || workunit?.isDeleted(),
            onClick: () => workunit?.setToFailed().catch(err => logger.error(err))
        },
        {
            key: "abort", text: nlsHPCC.Abort, disabled: workunit?.Archived || workunit?.isComplete() || workunit?.isDeleted(),
            onClick: () => workunit?.abort().catch(err => logger.error(err))
        },
        { key: "divider_4", itemType: ContextualMenuItemType.Divider },
        {
            key: "recover", text: nlsHPCC.Recover, disabled: workunit?.Archived || !workunit?.isComplete() || workunit?.isDeleted(),
            onClick: () => workunit?.recover().catch(err => logger.error(err))
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
        { key: "divider_5", itemType: ContextualMenuItemType.Divider },
        {
            key: "publish", text: nlsHPCC.Publish, disabled: !canDelete,
            onClick: () => setShowPublishForm(true)
        },
        { key: "divider_6", itemType: ContextualMenuItemType.Divider },
        {
            key: "zap", text: nlsHPCC.ZAP, disabled: !canDelete,
            onClick: () => setShowZapForm(true)
        },
        { key: "divider_7", itemType: ContextualMenuItemType.Divider },
        {
            key: "slaveLogs", text: nlsHPCC.SlaveLogs, disabled: !workunit?.ThorLogList,
            onClick: () => setShowThorSlaveLogs(true)
        },
    ], [wuProtected, canDelete, canDeschedule, canReschedule, canSave, description, jobname, refresh, refreshSavings, setShowDeleteConfirm, showMessageBar, workunit]);

    React.useEffect(() => {
        if (dockpanel && layout) {
            //  Should only happen once on startup  ---
            const dpLayout: any = dockpanel.getLayout();
            if (Array.isArray(dpLayout?.main?.sizes) && dpLayout.main.sizes.length === 2) {
                dpLayout.main.sizes = layout;
                dockpanel.layout(dpLayout).lazyRender();
            }
        }
    }, [dockpanel, layout]);

    React.useEffect(() => {
        return () => {
            if (dockpanel) {
                // While minimized the dock panel's real sizes are the collapsed
                // ones, so persist the pre-minimize sizes instead to avoid saving
                // a layout that looks minimized but reopens with the control unaware of it.
                const sizes = minimizedRef.current && preMinimizeSizes.current
                    ? preMinimizeSizes.current
                    : (dockpanel.getLayout() as any)?.main?.sizes;
                if (Array.isArray(sizes) && sizes.length === 2) {
                    setLayout(sizes as [number, number]);
                }
            }
        };
    }, [dockpanel, setLayout]);

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

    const engineRedirectSuffix = React.useMemo(() => {
        if (!engineRedirected)
            return "";
        return `${targetClusterType ? `${targetClusterType} ` : ""}${nlsHPCC.RedirectedByPickBestEngine}`;
    }, [engineRedirected, targetClusterType]);

    return <HolyGrail
        header={<>
            <CommandBar items={buttons} />
            {messageBarContent &&
                <MessageBar intent={messageBarContent.type}>
                    <MessageBarBody>{messageBarContent.message}</MessageBarBody>
                    <MessageBarActions containerAction={<Button onClick={dismissMessageBar} aria-label={nlsHPCC.Close} appearance="transparent" icon={<DismissRegular />} />} />
                </MessageBar>
            }
        </>}
        main={<>
            <DockPanel hideSingleTabs onCreate={setDockpanel}>
                <DockPanelItem key="summary" title="Summary">
                    <ScrollablePane scrollbarVisibility={ScrollbarVisibility.auto}>
                        <div className="pane-content">
                            <div className={styles.wuSummaryHeader}>
                                <div className={styles.wuPersona}>
                                    <WorkunitPersona wuid={wuid} />
                                </div>
                                {(jobname || workunit?.Owner) &&
                                    <div className={styles.linkWrapper}>
                                        {jobname &&
                                            <Link as="a" className={styles.jobNameLink} title={nlsHPCC.ViewWUsWithSimilarName} href={`#/workunits?Jobname=*${encodeURIComponent(jobname)}*`}>{jobname}</Link>
                                        }
                                        {jobname && workunit?.Owner &&
                                            <span className={styles.linkSeparator}>-</span>
                                        }
                                        {workunit?.Owner &&
                                            <Link as="a" className={styles.ownerLink} title={nlsHPCC.ViewWUsByOwner} href={`#/workunits?Owner=${encodeURIComponent(workunit?.Owner)}`}><PersonRegular className={styles.ownerIcon} />{workunit?.Owner}</Link>
                                        }
                                    </div>
                                }
                                <WUStatus wuid={wuid}></WUStatus>
                            </div>
                            <div className={styles.cardsWrapper}>
                                <Card className={styles.detailsPanel}>
                                    <TableGroup fields={{
                                        "state": { label: nlsHPCC.State, type: "string", value: workunit?.State + (workunit?.StateEx ? ` (${workunit.StateEx})` : ""), readonly: true },
                                        "action": { label: nlsHPCC.Action, type: "string", value: workunit?.ActionEx, readonly: true },
                                        "cluster": { label: nlsHPCC.Cluster, type: "string", value: workunit?.Cluster, readonly: true },
                                        ...(engineRedirected && engineRedirectSuffix ? { "clusterRedirect": { label: nlsHPCC.RedirectedTo, type: "string", value: engineRedirectSuffix, readonly: true } } : {}),
                                        "totalClusterTime": { label: nlsHPCC.TotalClusterTime, type: "string", value: workunit?.TotalClusterTime ? workunit?.TotalClusterTime : "0.00", readonly: true },
                                        "otel": { label: nlsHPCC.OpenTelemetry, type: "string", value: (otTraceId) ? `${otTraceId} / ${otSpanId}` : "", readonly: true, onCopy: () => copyToClipboard(JSON.stringify(parseOtTraceParent(otTraceParent))) },
                                        "owner": { label: nlsHPCC.Owner, type: "string", value: workunit?.Owner, readonly: true, onCopy: () => copyToClipboard(workunit?.Owner) },
                                        "jobname": { label: nlsHPCC.JobName, type: "string", value: jobname, onCopy: () => copyToClipboard(jobname) },
                                        "description": { label: nlsHPCC.Description, type: "string", value: description },
                                        "protected": { label: nlsHPCC.Protected, type: "checkbox", value: wuProtected },
                                        "ServiceNamesCustom": { label: nlsHPCC.Services, type: "string", value: serviceNames, readonly: true, multiline: true },
                                        "abortedBy": { label: nlsHPCC.AbortedBy, type: "string", value: workunit?.AbortBy, readonly: true },
                                        "abortedTime": { label: nlsHPCC.AbortedTime, type: "string", value: workunit?.AbortTime, readonly: true },
                                    }} onChange={(id, value) => {
                                        switch (id) {
                                            case "jobname":
                                                userEditedRef.current = true;
                                                setJobname(value);
                                                break;
                                            case "description":
                                                userEditedRef.current = true;
                                                setDescription(value);
                                                break;
                                            case "protected":
                                                userEditedRef.current = true;
                                                setWuProtected(value);
                                                break;
                                            default:
                                                logger.debug(`${id}:  ${value}`);
                                        }
                                    }} />
                                </Card>
                                <Card size="small" className={styles.costsCard}>
                                    <TableGroup fields={{
                                        "potentialSavings": { label: nlsHPCC.PotentialSavings, type: "string", value: `${formatCost(potentialSavings)} (${totalCosts > 0 ? Math.round((potentialSavings / totalCosts) * 10000) / 100 : 0}%)`, readonly: true },
                                        "compileCost": { label: nlsHPCC.CompileCost, type: "string", value: `${formatCost(workunit?.CompileCost)}`, readonly: true },
                                        "executeCost": { label: nlsHPCC.ExecuteCost, type: "string", value: `${formatCost(workunit?.ExecuteCost)}`, readonly: true },
                                        "fileAccessCost": { label: nlsHPCC.FileAccessCost, type: "string", value: `${formatCost(workunit?.FileAccessCost)}`, readonly: true },
                                    }} />
                                </Card>
                            </div>
                        </div>
                    </ScrollablePane>
                </DockPanelItem>
                <DockPanelItem key="errWarn" title="ErrWarn" padding={4} location="split-bottom" relativeTo="helpersTable">
                    <InfoGrid wuid={wuid} minimized={minimized} onMinimize={handleMinimize} onRestore={handleRestore}></InfoGrid>
                </DockPanelItem>
            </DockPanel>

            <PublishQueryForm wuid={wuid} showForm={showPublishForm} setShowForm={setShowPublishForm} />
            <ZAPDialog wuid={wuid} showForm={showZapForm} setShowForm={setShowZapForm} />
            <SlaveLogs wuid={wuid} showForm={showThorSlaveLogs} setShowForm={setShowThorSlaveLogs} />
            <DeleteConfirm />
        </>}
    />;
};
