import * as React from "react";
import { ScrollablePane, ScrollbarVisibility } from "./controls/ScrollablePane";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "./CommandBarV9";
import { Button, Card, Link, MessageBar, MessageBarActions, MessageBarBody, MessageBarIntent, SelectTabData, SelectTabEvent, Tab, TabList, makeStyles, tokens } from "@fluentui/react-components";
import { DismissRegular, PersonRegular } from "@fluentui/react-icons";
import { FileSprayStates, SashaService, WsSasha } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { SizeMe } from "../layouts/SizeMe";
import nlsHPCC from "src/nlsHPCC";
import { formatCost } from "src/Session";
import { copyToClipboard } from "src/Utility";
import { WorkunitPersona } from "./controls/StateIcon";
import { CommandMessages, FormatMessages, lfEncode } from "../comms/fileSpray";
import { useConfirm } from "../hooks/confirm";
import { useDfuWorkunit } from "../hooks/workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { DockPanel, DockPanelItem } from "../layouts/DockPanel";
import { pivotItemStyle } from "../layouts/pivot";
import { pushUrl, replaceUrl } from "../util/history";
import { Fields } from "./forms/Fields";
import { TableGroup } from "./forms/Groups";
import { XMLSourceEditor } from "./SourceEditor";

const logger = scopedLogger("../components/DFUWorkunitDetails.tsx");
const sashaService = new SashaService({ baseUrl: "" });

interface MessageBarContent {
    type: MessageBarIntent;
    message: string;
}

const useStyles = makeStyles({
    container: {
        height: "100%",
        position: "relative",
    },
    dfuWrapper: {
        containerType: "inline-size"
    },
    summaryHeader: {
        display: "flex",
        flexDirection: "row",
        flexWrap: "wrap",
        alignItems: "center",
        containerType: "inline-size",
        background: tokens.colorNeutralBackground1,
        borderBottom: `1px solid ${tokens.colorNeutralBackground1Pressed}`,
        marginBottom: "8px"
    },
    wuPersona: {
        marginBottom: "6px",
        "@container (max-width: 900px)": {
            flexBasis: "100%"
        }
    },
    linkWrapper: {
        margin: "0 10px 0 14px",
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
        display: "grid",
        gridTemplateColumns: "2fr 1fr",
        gap: "12px",
        margin: "0 4px 4px 4px",
        containerType: "inline-size",
        "@container (max-width: 1100px)": {
            gridTemplateColumns: "2fr 2fr"
        },
        "@container (max-width: 700px)": {
            gridTemplateColumns: "1fr"
        }
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
    rightCardsGrid: {
        display: "grid",
        gridTemplateColumns: "1fr",
        gap: "12px",
        "@container (max-width: 1100px)": {
            gridTemplateColumns: "1fr",
        },
        "& h4": {
            margin: 0
        },
        "& .fui-Card": {
            gap: 0
        }
    },
});

type FieldMap = { key: string, label: string };
const sourceFieldIds: FieldMap[] = [
    { key: "SourceIP", label: nlsHPCC.IP },
    { key: "SourceDirectory", label: nlsHPCC.Directory },
    { key: "SourceFilePath", label: nlsHPCC.FilePath },
    { key: "SourceLogicalName", label: nlsHPCC.LogicalName },
    { key: "SourceNumParts", label: nlsHPCC.NumberofParts },
    { key: "SourceDali", label: nlsHPCC.Dali },
    { key: "SourceFormat", label: nlsHPCC.Format },
    { key: "SourceRecordSize", label: nlsHPCC.RecordSize },
    { key: "RowTag", label: nlsHPCC.RowTag },
    { key: "SourceCsvSeparate", label: nlsHPCC.Separators },
    { key: "SourceCsvEscape", label: nlsHPCC.Escape },
    { key: "SourceCsvTerminate", label: nlsHPCC.Terminators },
    { key: "SourceCsvQuote", label: nlsHPCC.Quote }
];
const targetFieldIds: FieldMap[] = [
    { key: "DestIP", label: nlsHPCC.IP },
    { key: "DestDirectory", label: nlsHPCC.Directory },
    { key: "DestFilePath", label: nlsHPCC.FilePath },
    { key: "DestLogicalName", label: nlsHPCC.LogicalName },
    { key: "DestGroupName", label: nlsHPCC.GroupName },
    { key: "DestNumParts", label: nlsHPCC.NumberofParts },
    { key: "DestFormat", label: nlsHPCC.Format },
    { key: "DestRecordSize", label: nlsHPCC.RecordSize }
];

interface DFUWorkunitDetailsProps {
    wuid: string;
    tab?: string;
}

export const DFUWorkunitDetails: React.FunctionComponent<DFUWorkunitDetailsProps> = ({
    wuid,
    tab = "summary"
}) => {

    const [workunit, , , , refresh] = useDfuWorkunit(wuid, true);
    const [wuXML, setWuXML] = React.useState("");
    const [jobname, setJobname] = React.useState("");
    const [sourceFormatMessage, setSourceFormatMessage] = React.useState("");
    const [sourceFields, setSourceFields] = React.useState<Fields>();
    const [targetFormatMessage, setTargetFormatMessage] = React.useState("");
    const [targetFields, setTargetFields] = React.useState<Fields>();
    const [_protected, setProtected] = React.useState(false);

    const [messageBarContent, setMessageBarContent] = React.useState<MessageBarContent | undefined>();
    const dismissMessageBar = React.useCallback(() => setMessageBarContent(undefined), []);
    const showMessageBar = React.useCallback((content: MessageBarContent) => {
        setMessageBarContent(content);
        const t = window.setTimeout(function () {
            dismissMessageBar();
            window.clearTimeout(t);
        }, 2400);
    }, [dismissMessageBar]);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.YouAreAboutToDeleteThisWorkunit,
        onSubmit: React.useCallback(() => {
            workunit?.delete()
                .then(response => {
                    replaceUrl("/dfuworkunits");
                })
                .catch(err => logger.error(err))
                ;
        }, [workunit])
    });

    React.useEffect(() => {
        if (!workunit) return;
        workunit?.fetchXML().then(xml => {
            setWuXML(xml);
        }).catch(err => logger.error(err));
    }, [workunit]);

    React.useEffect(() => {
        if (!workunit) return;
        setJobname(workunit?.JobName);
        setProtected(workunit?.isProtected);

        const sourceFormatMsg = FormatMessages[workunit?.SourceFormat];
        if (sourceFormatMsg === "csv") {
            setSourceFormatMessage(`(${nlsHPCC.CSV})`);
        } else if (sourceFormatMsg === "fixed") {
            setSourceFormatMessage(`(${nlsHPCC.Fixed})`);
        } else if (workunit?.RowTag) {
            setSourceFormatMessage(`(${nlsHPCC.XML}/${nlsHPCC.JSON})`);
        }

        const _sourceFields: Fields = {};
        for (const fieldId of sourceFieldIds) {
            if (workunit[fieldId.key] !== undefined) {
                const value = fieldId.key === "SourceFormat" ? FormatMessages[workunit[fieldId.key]] : workunit[fieldId.key];
                if (fieldId.key === "SourceFilePath") {
                    const href = `#/landingzone/preview/~file::${workunit["SourceIP"]}::${lfEncode(value)}`;
                    _sourceFields[fieldId.key] = { label: fieldId.label, value: value ?? null, type: "link", href, readonly: true };
                } else if (fieldId.key === "SourceLogicalName" && value) {
                    _sourceFields[fieldId.key] = { label: fieldId.label, value, type: "link", href: `#/files/${value}`, readonly: true };
                } else {
                    _sourceFields[fieldId.key] = { label: fieldId.label, value: value ?? null, type: "string", readonly: true };
                }
            }
        }
        setSourceFields(_sourceFields);

        const destFormatMsg = FormatMessages[workunit?.DestFormat];
        if (destFormatMsg === "csv") {
            setTargetFormatMessage(`(${nlsHPCC.CSV})`);
        } else if (destFormatMsg === "fixed") {
            setTargetFormatMessage(`(${nlsHPCC.Fixed})`);
        }

        const _targetFields: Fields = {};
        for (const fieldId of targetFieldIds) {
            if (workunit[fieldId.key] !== undefined) {
                const value = fieldId.key === "DestFormat" ? FormatMessages[workunit[fieldId.key]] : workunit[fieldId.key];
                if (fieldId.key === "DestLogicalName") {
                    _targetFields[fieldId.key] = { label: fieldId.label, value: value ?? null, type: "link", href: `#/files/${value}`, readonly: true };
                } else {
                    _targetFields[fieldId.key] = { label: fieldId.label, value: value ?? null, type: "string", readonly: true };
                }
            }
        }
        setTargetFields(_targetFields);
    }, [workunit]);

    const canSave = React.useMemo(() => {
        return jobname !== workunit?.JobName || _protected !== workunit?.isProtected;
    }, [jobname, _protected, workunit?.JobName, workunit?.isProtected]);

    const canDelete = React.useMemo(() => {
        return _protected !== workunit?.isProtected && 999 !== workunit?.State && workunit?.Archived;
    }, [_protected, workunit?.isProtected, workunit?.State, workunit?.Archived]);

    const canAbort = React.useMemo(() => {
        return !workunit?.isComplete() && !workunit?.isDeleted();
    }, [workunit]);

    const saveWorkunit = React.useCallback(() => {
        workunit?.update({ wu: { JobName: jobname, isProtected: _protected } })
            .then(_ => {
                workunit.refresh();
                showMessageBar({ type: "success", message: nlsHPCC.SuccessfullySaved });
            })
            .catch(err => logger.error(err));
    }, [jobname, _protected, workunit, showMessageBar]);

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => { refresh(true); }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider },
        {
            key: "save", text: nlsHPCC.Save, iconProps: { iconName: "Save" }, disabled: !canSave,
            onClick: saveWorkunit
        },
        {
            key: "delete", text: nlsHPCC.Delete, iconProps: { iconName: "Delete" }, disabled: canDelete,
            onClick: () => setShowDeleteConfirm(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider },
        {
            key: "restore", text: nlsHPCC.Restore,
            onClick: () => {
                sashaService.RestoreWU({
                    Wuid: wuid,
                    WUType: WsSasha.WUTypes.DFU
                }).then(() => {
                    refresh();
                }).catch(err => {
                    logger.error(err);
                });
            }
        },
        {
            key: "abort", text: nlsHPCC.Abort, disabled: canAbort,
            onClick: () => { workunit?.abort().catch(err => logger.error(err)); }
        },
    ], [canAbort, canDelete, canSave, refresh, saveWorkunit, setShowDeleteConfirm, workunit, wuid]);

    const onTabSelect = React.useCallback((_: SelectTabEvent, data: SelectTabData) => {
        const nextTab = data.value as string;
        if (nextTab === "target") {
            pushUrl(`/files/${workunit?.DestGroupName}/${workunit?.DestLogicalName}`);
        } else {
            pushUrl(`/dfuworkunits/${wuid}/${nextTab}`);
        }
    }, [workunit?.DestGroupName, workunit?.DestLogicalName, wuid]);

    const styles = useStyles();

    return <>
        <SizeMe>{({ size }) =>
            <div className={styles.container}>
                <TabList selectedValue={tab} onTabSelect={onTabSelect} size="medium">
                    <Tab value="summary">{wuid}</Tab>
                    <Tab value="xml">{nlsHPCC.XML}</Tab>
                    <Tab value="target">{nlsHPCC.Target}</Tab>
                </TabList>
                {tab === "summary" &&
                    <div style={pivotItemStyle(size)}>
                        <HolyGrail
                            header={<>
                                <CommandBar items={buttons} />
                                {messageBarContent &&
                                    <MessageBar intent={messageBarContent.type}>
                                        <MessageBarBody>{messageBarContent.message}</MessageBarBody>
                                        <MessageBarActions containerAction={<Button onClick={dismissMessageBar} aria-label={nlsHPCC.Close} appearance="transparent" icon={<DismissRegular />} />} />
                                    </MessageBar>
                                }
                            </>}
                            main={
                                <DockPanel hideSingleTabs>
                                    <DockPanelItem key="summary" title="Summary">
                                        <ScrollablePane scrollbarVisibility={ScrollbarVisibility.auto}>
                                            <div className={`${styles.dfuWrapper} pane-content`}>
                                                <div className={styles.summaryHeader}>
                                                    <div className={styles.wuPersona}>
                                                        <WorkunitPersona wuid={wuid} />
                                                    </div>
                                                    {(jobname || workunit?.User) &&
                                                        <div className={styles.linkWrapper}>
                                                            {jobname &&
                                                                <Link as="a" className={styles.jobNameLink} title={nlsHPCC.ViewWUsWithSimilarName} href={`#/dfuworkunits?Jobname=${encodeURIComponent(jobname)}`}>{jobname}</Link>
                                                            }
                                                            {jobname && workunit?.User &&
                                                                <span className={styles.linkSeparator}>-</span>
                                                            }
                                                            {workunit?.User &&
                                                                <Link as="a" className={styles.ownerLink} title={nlsHPCC.ViewWUsByOwner} href={`#/dfuworkunits?Owner=${encodeURIComponent(workunit?.User)}`}><PersonRegular className={styles.ownerIcon} />{workunit?.User}</Link>
                                                            }
                                                        </div>
                                                    }
                                                </div>
                                                <div className={styles.cardsWrapper}>
                                                    <Card className={styles.detailsPanel}>
                                                        <TableGroup fields={{
                                                            "clusterName": { label: nlsHPCC.ClusterName, type: "string", value: workunit?.ClusterName, readonly: true },
                                                            "jobname": { label: nlsHPCC.JobName, type: "string", value: jobname, onCopy: () => copyToClipboard(jobname) },
                                                            "dfuServerName": { label: nlsHPCC.DFUServerName, type: "string", value: workunit?.DFUServerName, readonly: true },
                                                            "queue": { label: nlsHPCC.Queue, type: "string", value: workunit?.Queue, readonly: true },
                                                            "user": { label: nlsHPCC.User, type: "string", value: workunit?.User, readonly: true, onCopy: () => copyToClipboard(workunit?.User) },
                                                            "protected": { label: nlsHPCC.Protected, type: "checkbox", value: _protected },
                                                            "command": { label: nlsHPCC.Command, type: "string", value: CommandMessages[workunit?.Command], readonly: true },
                                                            "state": { label: nlsHPCC.State, type: "string", value: workunit?.StateReason ? `${FileSprayStates[workunit?.State]} - ${workunit?.StateReason}` : FileSprayStates[workunit?.State], readonly: true },
                                                            "fileAccessCost": { label: nlsHPCC.FileAccessCost, type: "string", value: `${formatCost(workunit?.FileAccessCost ?? 0)}`, readonly: true },
                                                            "timeStarted": { label: nlsHPCC.TimeStarted, type: "string", value: workunit?.TimeStarted, readonly: true },
                                                            "secondsLeft": { label: nlsHPCC.SecondsRemaining, type: "number", value: workunit?.SecsLeft, readonly: true },
                                                            "timeStopped": { label: nlsHPCC.TimeStopped, type: "string", value: workunit?.TimeStopped, readonly: true },
                                                            "percentDone": { label: nlsHPCC.PercentDone, type: "progress", value: workunit?.PercentDone?.toString(), readonly: true },
                                                            "progressMessage": { label: nlsHPCC.ProgressMessage, type: "string", value: workunit?.ProgressMessage, readonly: true },
                                                            "summaryMessage": { label: nlsHPCC.SummaryMessage, type: "string", value: workunit?.SummaryMessage, readonly: true },
                                                        }} onChange={(id, value) => {
                                                            switch (id) {
                                                                case "jobname":
                                                                    setJobname(value);
                                                                    break;
                                                                case "protected":
                                                                    setProtected(value);
                                                                    break;
                                                                default:
                                                                    logger.debug(`${id}:  ${value}`);
                                                            }
                                                        }} />
                                                    </Card>
                                                    <div className={styles.rightCardsGrid}>
                                                        <Card>
                                                            <h4>{nlsHPCC.Source} {sourceFormatMessage}</h4>
                                                            <TableGroup fields={sourceFields} />
                                                        </Card>
                                                        <Card>
                                                            <h4>{nlsHPCC.Target} {targetFormatMessage}</h4>
                                                            <TableGroup fields={targetFields} />
                                                        </Card>
                                                        <Card>
                                                            <h4>{nlsHPCC.Other}</h4>
                                                            <TableGroup fields={{
                                                                "monitorSub": { label: nlsHPCC.MonitorSub, type: "string", value: workunit?.MonitorSub ? "true" : "false", readonly: true },
                                                                "overwrite": { label: nlsHPCC.Overwrite, type: "string", value: workunit?.Overwrite ? "true" : "false", readonly: true },
                                                                "replicate": { label: nlsHPCC.Replicate, type: "string", value: workunit?.Replicate ? "true" : "false", readonly: true },
                                                                "compress": { label: nlsHPCC.Compress, type: "string", value: workunit?.Compress ? "true" : "false", readonly: true },
                                                            }} />
                                                        </Card>
                                                    </div>
                                                </div>
                                            </div>
                                        </ScrollablePane>
                                    </DockPanelItem>
                                </DockPanel>
                            }
                        />
                    </div>
                }
                {tab === "xml" &&
                    <div style={pivotItemStyle(size, 0)}>
                        <XMLSourceEditor text={wuXML} readonly={true} />
                    </div>
                }
            </div>
        }</SizeMe>
        <DeleteConfirm />
    </>;
};