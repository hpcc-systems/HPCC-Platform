import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Pivot, PivotItem, Sticky, StickyPositionType } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import * as FileSpray from "src/FileSpray";
import * as ESPDFUWorkunit from "src/ESPDFUWorkunit";
import { useFavorite } from "../hooks/favorite";
import { pivotItemStyle } from "../layouts/pivot";
import { pushUrl } from "../util/history";
import { ShortVerticalDivider } from "./Common";
import { TableGroup } from "./forms/Groups";
import { XMLSourceEditor } from "./SourceEditor";

interface DFUWorkunitDetailsProps {
    wuid: string;
    tab?: string;
}

export const DFUWorkunitDetails: React.FunctionComponent<DFUWorkunitDetailsProps> = ({
    wuid,
    tab = "summary"
}) => {

    const [workunit, setWorkunit] = React.useState<any>(null);
    const [dfuWuData, setDfuWuData] = React.useState<any>(null);
    const [wuXML, setWuXML] = React.useState("");
    const [jobname, setJobname] = React.useState("");
    const [_protected, setProtected] = React.useState(false);
    const [isFavorite, addFavorite, removeFavorite] = useFavorite(window.location.hash);

    React.useEffect(() => {
        setWorkunit(ESPDFUWorkunit.Get(wuid));
        FileSpray.GetDFUWorkunit({ request: { wuid }}).then(response => {
            setDfuWuData(response?.GetDFUWorkunitResponse?.result);
        });
    }, [wuid]);

    React.useEffect(() => {
        if (!dfuWuData) return;
        setJobname(dfuWuData?.JobName);
        setProtected(dfuWuData?.isProtected);
    }, [dfuWuData]);

    React.useEffect(() => {
        if (!workunit) return;
        workunit?.fetchXML(function (response) {
            setWuXML(response);
        });
    }, [workunit]);

    const canSave = dfuWuData && (
        jobname !== dfuWuData?.JobName ||
        _protected !== dfuWuData?.isProtected
    );

    const canDelete = dfuWuData && (
        _protected !== dfuWuData?.Protected &&
        999 !== dfuWuData?.StateID &&
        dfuWuData?.Archived
    );

    const canAbort = workunit && (
        !workunit?.isComplete() &&
        !workunit?.isDeleted()
    );

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => workunit?.refresh()
        },
        {
            key: "copy", text: nlsHPCC.CopyWUID, iconProps: { iconName: "Copy" },
            onClick: () => navigator?.clipboard?.writeText(wuid)
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "save", text: nlsHPCC.Save, iconProps: { iconName: "Save" }, disabled: !canSave,
            onClick: () => workunit?.update({ JobName: jobname, isProtected: _protected })
        },
        {
            key: "delete", text: nlsHPCC.Delete, iconProps: { iconName: "Delete" }, disabled: canDelete,
            onClick: () => {
                if (confirm(nlsHPCC.YouAreAboutToDeleteThisWorkunit)) {
                    workunit?.doDelete();
                    pushUrl("/dfuworkunits");
                }
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "abort", text: nlsHPCC.Abort, disabled: canAbort,
            onClick: () => workunit?.abort()
        },
    ], [_protected, canAbort, canDelete, canSave, jobname, workunit, wuid]);

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

    return <SizeMe monitorHeight>{({ size }) =>
        <Pivot
            overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab}
            onLinkClick={evt => {
                if (evt.props.itemKey === "target") {
                    pushUrl(`/files/${dfuWuData?.DestGroupName}/${dfuWuData?.DestLogicalName}`);
                } else {
                    pushUrl(`/dfuworkunits/${wuid}/${evt.props.itemKey}`);
                }
            }}
        >
            <PivotItem headerText={wuid} itemKey="summary" style={pivotItemStyle(size)} >
                <Sticky stickyPosition={StickyPositionType.Header}>
                    <CommandBar items={buttons} farItems={rightButtons} />
                </Sticky>
                <TableGroup fields={{
                    "id": { label: nlsHPCC.ID, type: "string", value: wuid, readonly: true },
                    "clusterName": { label: nlsHPCC.ClusterName, type: "string", value: dfuWuData?.ClusterName, readonly: true },
                    "jobname": { label: nlsHPCC.JobName, type: "string", value: jobname },
                    "dfuServerName": { label: nlsHPCC.DFUServerName, type: "string", value: dfuWuData?.DFUServerName, readonly: true },
                    "queue": { label: nlsHPCC.Queue, type: "string", value: dfuWuData?.Queue, readonly: true },
                    "user": { label: nlsHPCC.User, type: "string", value: dfuWuData?.Owner, readonly: true },
                    "protected": { label: nlsHPCC.Protected, type: "checkbox", value: _protected },
                    "command": { label: nlsHPCC.Command, type: "string", value: FileSpray.CommandMessages[dfuWuData?.Command], readonly: true },
                    "state": { label: nlsHPCC.State, type: "string", value: FileSpray.States[dfuWuData?.State], readonly: true },
                    "timeStarted": { label: nlsHPCC.TimeStarted, type: "string", value: dfuWuData?.TimeStarted, readonly: true },
                    "timeStopped": { label: nlsHPCC.TimeStopped, type: "string", value: dfuWuData?.TimeStopped, readonly: true },
                    "percentDone": { label: nlsHPCC.PercentDone, type: "progress", value: dfuWuData?.PercentDone, readonly: true },
                    "progressMessage": { label: nlsHPCC.ProgressMessage, type: "string", value: dfuWuData?.ProgressMessage, readonly: true },
                    "summaryMessage": { label: nlsHPCC.SummaryMessage, type: "string", value: dfuWuData?.SummaryMessage, readonly: true },
                }} onChange={(id, value) => {
                    switch (id) {
                        case "jobname":
                            setJobname(value);
                            break;
                        case "protected":
                            setProtected(value);
                            break;
                        default:
                            console.log(id, value);
                    }
                }} />
                <hr />
                <h2>{nlsHPCC.Source} ({nlsHPCC.Fixed})</h2>
                <TableGroup fields={{
                    "ip": { label: nlsHPCC.IP, type: "string", value: dfuWuData?.SourceIP, readonly: true },
                    "directory": { label: nlsHPCC.Directory, type: "string", value: dfuWuData?.SourceDirectory, readonly: true },
                    "filePath": { label: nlsHPCC.FilePath, type: "string", value: dfuWuData?.SourceFilePath, readonly: true },
                    "numParts": { label: nlsHPCC.NumberofParts, type: "string", value: dfuWuData?.SourceNumParts, readonly: true },
                    "format": { label: nlsHPCC.Format, type: "string", value: FileSpray.FormatMessages[dfuWuData?.SourceFormat], readonly: true },
                    "recordSize": { label: nlsHPCC.RecordSize, type: "string", value: dfuWuData?.SourceRecordSize, readonly: true },
                }} />
                <hr />
                <h2>{nlsHPCC.Target}</h2>
                <TableGroup fields={{
                    "directory": { label: nlsHPCC.Directory, type: "string", value: dfuWuData?.DestDirectory, readonly: true },
                    "logicalName": { label: nlsHPCC.LogicalName, type: "string", value: dfuWuData?.DestLogicalName, readonly: true },
                    "groupName": { label: nlsHPCC.GroupName, type: "string", value: dfuWuData?.DestGroupName, readonly: true },
                    "numParts": { label: nlsHPCC.NumberofParts, type: "string", value: dfuWuData?.DestNumParts, readonly: true },
                }} />
                <hr />
                <h2>{nlsHPCC.Other}</h2>
                <TableGroup fields={{
                    "monitorSub": { label: nlsHPCC.MonitorSub, type: "string", value: dfuWuData?.MonitorSub ? "true" : "false", readonly: true },
                    "overwrite": { label: nlsHPCC.Overwrite, type: "string", value: dfuWuData?.Overwrite ? "true" : "false", readonly: true },
                    "replicate": { label: nlsHPCC.Replicate, type: "string", value: dfuWuData?.Replicate ? "true" : "false", readonly: true },
                    "compress": { label: nlsHPCC.Compress, type: "string", value: dfuWuData?.Compress ? "true" : "false", readonly: true },
                }} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.XML} itemKey="xml" style={pivotItemStyle(size, 0)}>
                <XMLSourceEditor text={wuXML} readonly={true} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Target} itemKey="target"></PivotItem>
        </Pivot>
    }</SizeMe>;
};