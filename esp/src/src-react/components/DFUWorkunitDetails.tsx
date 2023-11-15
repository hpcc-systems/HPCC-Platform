import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, MessageBar, MessageBarType, Pivot, PivotItem, Sticky, StickyPositionType } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import * as FileSpray from "src/FileSpray";
import { useConfirm } from "../hooks/confirm";
import { useDfuWorkunit } from "../hooks/workunit";
import { pivotItemStyle } from "../layouts/pivot";
import { pushUrl, replaceUrl } from "../util/history";
import { ShortVerticalDivider } from "./Common";
import { Field, Fields } from "./forms/Fields";
import { TableGroup } from "./forms/Groups";
import { XMLSourceEditor } from "./SourceEditor";

const logger = scopedLogger("../components/DFUWorkunitDetails.tsx");

const createField = (label: string, value: any): Field => {
    return { label, type: typeof value === "number" ? "number" : "string", value, readonly: true };
};

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

    const [showMessageBar, setShowMessageBar] = React.useState(false);
    const dismissMessageBar = React.useCallback(() => setShowMessageBar(false), []);

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

        const sourceFormatMsg = FileSpray.FormatMessages[workunit?.SourceFormat];
        if (sourceFormatMsg === "csv") {
            setSourceFormatMessage(`(${nlsHPCC.CSV})`);
        } else if (sourceFormatMsg === "fixed") {
            setSourceFormatMessage(`(${nlsHPCC.Fixed})`);
        } else if (!!workunit?.RowTag) {
            setSourceFormatMessage(`(${nlsHPCC.XML}/${nlsHPCC.JSON})`);
        }

        const _sourceFields: Fields = {};
        for (const fieldId of sourceFieldIds) {
            if (workunit[fieldId.key] !== undefined) {
                const value = fieldId.key === "SourceFormat" ? FileSpray.FormatMessages[workunit[fieldId.key]] : workunit[fieldId.key];
                _sourceFields[fieldId.key] = createField(fieldId.label, value ?? null);
            }
        }
        setSourceFields(_sourceFields);

        const destFormatMsg = FileSpray.FormatMessages[workunit?.DestFormat];
        if (destFormatMsg === "csv") {
            setTargetFormatMessage(`(${nlsHPCC.CSV})`);
        } else if (destFormatMsg === "fixed") {
            setTargetFormatMessage(`(${nlsHPCC.Fixed})`);
        }

        const _targetFields: Fields = {};
        for (const fieldId of targetFieldIds) {
            if (workunit[fieldId.key] !== undefined) {
                const value = fieldId.key === "DestFormat" ? FileSpray.FormatMessages[workunit[fieldId.key]] : workunit[fieldId.key];
                _targetFields[fieldId.key] = createField(fieldId.label, value ?? null);
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
                setShowMessageBar(true);
                workunit.refresh();
                const t = window.setTimeout(function () {
                    setShowMessageBar(false);
                    window.clearTimeout(t);
                }, 2400);
            })
            .catch(err => logger.error(err));
    }, [jobname, _protected, workunit]);

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refresh()
        },
        {
            key: "copy", text: nlsHPCC.CopyWUID, iconProps: { iconName: "Copy" },
            onClick: () => { navigator?.clipboard?.writeText(wuid); }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "save", text: nlsHPCC.Save, iconProps: { iconName: "Save" }, disabled: !canSave,
            onClick: saveWorkunit
        },
        {
            key: "delete", text: nlsHPCC.Delete, iconProps: { iconName: "Delete" }, disabled: canDelete,
            onClick: () => setShowDeleteConfirm(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "abort", text: nlsHPCC.Abort, disabled: canAbort,
            onClick: () => { workunit?.abort().catch(err => logger.error(err)); }
        },
    ], [canAbort, canDelete, canSave, refresh, saveWorkunit, setShowDeleteConfirm, workunit, wuid]);

    return <>
        <SizeMe monitorHeight>{({ size }) =>
            <Pivot
                overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab}
                onLinkClick={evt => {
                    if (evt.props.itemKey === "target") {
                        pushUrl(`/files/${workunit?.DestGroupName}/${workunit?.DestLogicalName}`);
                    } else {
                        pushUrl(`/dfuworkunits/${wuid}/${evt.props.itemKey}`);
                    }
                }}
            >
                <PivotItem headerText={wuid} itemKey="summary" style={pivotItemStyle(size)} >
                    <Sticky stickyPosition={StickyPositionType.Header}>
                        <CommandBar items={buttons} />
                        {showMessageBar &&
                            <MessageBar
                                messageBarType={MessageBarType.success}
                                dismissButtonAriaLabel={nlsHPCC.Close}
                                onDismiss={dismissMessageBar}
                            >
                                {nlsHPCC.SuccessfullySaved}
                            </MessageBar>
                        }
                    </Sticky>
                    <TableGroup fields={{
                        "id": { label: nlsHPCC.ID, type: "string", value: wuid, readonly: true },
                        "clusterName": { label: nlsHPCC.ClusterName, type: "string", value: workunit?.ClusterName, readonly: true },
                        "jobname": { label: nlsHPCC.JobName, type: "string", value: jobname },
                        "dfuServerName": { label: nlsHPCC.DFUServerName, type: "string", value: workunit?.DFUServerName, readonly: true },
                        "queue": { label: nlsHPCC.Queue, type: "string", value: workunit?.Queue, readonly: true },
                        "user": { label: nlsHPCC.User, type: "string", value: workunit?.User, readonly: true },
                        "protected": { label: nlsHPCC.Protected, type: "checkbox", value: _protected },
                        "command": { label: nlsHPCC.Command, type: "string", value: FileSpray.CommandMessages[workunit?.Command], readonly: true },
                        "state": { label: nlsHPCC.State, type: "string", value: FileSpray.States[workunit?.State], readonly: true },
                        "timeStarted": { label: nlsHPCC.TimeStarted, type: "string", value: workunit?.TimeStarted, readonly: true },
                        "secondsLeft": { label: nlsHPCC.SecondsRemaining, type: "number", value: workunit?.SecsLeft, readonly: true },
                        "timeStopped": { label: nlsHPCC.TimeStopped, type: "string", value: workunit?.TimeStopped, readonly: true },
                        "percentDone": { label: nlsHPCC.PercentDone, type: "progress", value: workunit?.PercentDone.toString(), readonly: true },
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
                    <hr />
                    <h2>{nlsHPCC.Source} {sourceFormatMessage}</h2>
                    <TableGroup fields={sourceFields} />
                    <hr />
                    <h2>{nlsHPCC.Target} {targetFormatMessage}</h2>
                    <TableGroup fields={targetFields} />
                    <hr />
                    <h2>{nlsHPCC.Other}</h2>
                    <TableGroup fields={{
                        "monitorSub": { label: nlsHPCC.MonitorSub, type: "string", value: workunit?.MonitorSub ? "true" : "false", readonly: true },
                        "overwrite": { label: nlsHPCC.Overwrite, type: "string", value: workunit?.Overwrite ? "true" : "false", readonly: true },
                        "replicate": { label: nlsHPCC.Replicate, type: "string", value: workunit?.Replicate ? "true" : "false", readonly: true },
                        "compress": { label: nlsHPCC.Compress, type: "string", value: workunit?.Compress ? "true" : "false", readonly: true },
                    }} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.XML} itemKey="xml" style={pivotItemStyle(size, 0)}>
                    <XMLSourceEditor text={wuXML} readonly={true} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.Target} itemKey="target"></PivotItem>
            </Pivot>
        }</SizeMe>
        <DeleteConfirm />
    </>;
};