import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Pivot, PivotItem, ScrollablePane, ScrollbarVisibility, Sticky, StickyPositionType } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { FileParts } from "./FileParts";
import * as Utility from "src/Utility";
import { getStateImageName, IFile } from "src/ESPLogicalFile";
import { useFile, useDefFile } from "../hooks/File";
import { pivotItemStyle } from "../layouts/pivot";
import { DojoAdapter } from "../layouts/DojoAdapter";
import { pushUrl } from "../util/history";
import { FileBlooms } from "./FileBlooms";
import { SuperFiles } from "./SuperFiles";
import { ECLSourceEditor, XMLSourceEditor } from "./SourceEditor";
import { ShortVerticalDivider } from "./Common";
import { FileDetailsGraph } from "./FileDetailsGraph";
import { TableGroup } from "./forms/Groups";
import { Queries } from "./Queries";
import { WorkunitDetails } from "./WorkunitDetails";
import { DataPatternsReport } from "./DataPatternsReport";

import "react-reflex/styles.css";

interface FileDetailsProps {
    cluster?: string;
    logicalFile: string;
    tab?: string;
}

export const FileDetails: React.FunctionComponent<FileDetailsProps> = ({
    cluster,
    logicalFile,
    tab = "summary"
}) => {

    const [file, , refresh] = useFile(cluster, logicalFile);
    const [defFile] = useDefFile(cluster, logicalFile, "def");
    const [xmlFile] = useDefFile(cluster, logicalFile, "xml");
    const [description, setDescription] = React.useState("");
    const [_protected, setProtected] = React.useState(false);
    const [restricted, setRestricted] = React.useState(false);

    const isDFUWorkunit = file?.Wuid[0] === "D";
    const isProtected = file?.ProtectList?.DFUFileProtect?.length > 0 || false;

    React.useEffect(() => {
        setDescription(description || file?.Description);
        setProtected(_protected || isProtected);
        setRestricted(restricted || file?.IsRestricted);

    }, [_protected, description, file?.Description, file?.IsRestricted, isProtected, restricted]);

    const canSave = file && (
        description !== file.Description ||
        _protected !== isProtected ||
        restricted !== file?.IsRestricted
    );

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => {
                refresh();
            }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "save", text: nlsHPCC.Save, iconProps: { iconName: "Save" }, disabled: !canSave,
            onClick: () => {
                file?.update({
                    UpdateDescription: true,
                    FileDesc: description,
                    Protect: _protected ? "1" : "2",
                    Restrict: restricted ? "1" : "2",
                });
            }
        },
        {
            key: "copy", text: nlsHPCC.CopyLogicalFilename, iconProps: { iconName: "Copy" },
            onClick: () => {
                navigator?.clipboard?.writeText(logicalFile);
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
    ], [_protected, canSave, description, file, logicalFile, refresh, restricted]);

    const protectedImage = _protected ? Utility.getImageURL("locked.png") : Utility.getImageURL("unlocked.png");
    const stateImage = Utility.getImageURL(getStateImageName(file as unknown as IFile));
    const compressedImage = file?.IsCompressed ? Utility.getImageURL("compressed.png") : "";

    return <SizeMe monitorHeight>{({ size }) =>
        <Pivot overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab} onLinkClick={evt => pushUrl(`/files/${cluster}/${logicalFile}/${evt.props.itemKey}`)}>
            <PivotItem headerText={nlsHPCC.Summary} itemKey="summary" style={pivotItemStyle(size)}>
                <ScrollablePane scrollbarVisibility={ScrollbarVisibility.auto}>
                    <Sticky stickyPosition={StickyPositionType.Header}>
                        <CommandBar items={buttons} />
                    </Sticky>
                    <Sticky stickyPosition={StickyPositionType.Header}>
                        <div style={{ display: "inline-block" }}>
                            <h2>
                                <img src={compressedImage} />&nbsp;
                                <img src={protectedImage} />&nbsp;
                                <img src={stateImage} />&nbsp;
                                {file?.Name}
                            </h2>
                        </div>
                    </Sticky>
                    <TableGroup fields={{
                        "Wuid": { label: nlsHPCC.Workunit, type: "link", value: file?.Wuid, href: `#/${isDFUWorkunit ? "dfu" : ""}workunits/${file?.Wuid}`, readonly: true, },
                        "Owner": { label: nlsHPCC.Owner, type: "string", value: file?.Owner, readonly: true },
                        "SuperOwner": { label: nlsHPCC.SuperFile, type: "links", links: file?.Superfiles?.DFULogicalFile?.map(row => ({ label: "", type: "link", value: row.Name, href: `#/superfiles/${row.Name}` })) },
                        "NodeGroup": { label: nlsHPCC.ClusterName, type: "string", value: file?.NodeGroup, readonly: true },
                        "Description": { label: nlsHPCC.Description, type: "string", value: description },
                        "JobName": { label: nlsHPCC.JobName, type: "string", value: file?.JobName, readonly: true },
                        "isProtected": { label: nlsHPCC.Protected, type: "checkbox", value: _protected },
                        "isRestricted": { label: nlsHPCC.Restricted, type: "checkbox", value: restricted },
                        "ContentType": { label: nlsHPCC.ContentType, type: "string", value: file?.ContentType, readonly: true },
                        "KeyType": { label: nlsHPCC.KeyType, type: "string", value: file?.KeyType, readonly: true },
                        "Filesize": { label: nlsHPCC.FileSize, type: "string", value: file?.Filesize, readonly: true },
                        "Format": { label: nlsHPCC.Format, type: "string", value: file?.Format, readonly: true },
                        "IsCompressed": { label: nlsHPCC.IsCompressed, type: "checkbox", value: file?.IsCompressed, readonly: true },
                        "CompressedFileSizeString": { label: nlsHPCC.CompressedFileSize, type: "string", value: file?.CompressedFileSize ? file?.CompressedFileSize.toString() : "", readonly: true },
                        "PercentCompressed": { label: nlsHPCC.PercentCompressed, type: "string", value: file?.PercentCompressed, readonly: true },
                        "Modified": { label: nlsHPCC.Modified, type: "string", value: file?.Modified, readonly: true },
                        "ExpireDays": { label: nlsHPCC.ExpireDays, type: "string", value: file?.ExpireDays ? file?.ExpireDays.toString() : "", readonly: true },
                        "Directory": { label: nlsHPCC.Directory, type: "string", value: file?.Dir, readonly: true },
                        "PathMask": { label: nlsHPCC.PathMask, type: "string", value: file?.PathMask, readonly: true },
                        "RecordSize": { label: nlsHPCC.RecordSize, type: "string", value: file?.RecordSize, readonly: true },
                        "RecordCount": { label: nlsHPCC.RecordCount, type: "string", value: file?.RecordCount, readonly: true },
                        "IsReplicated": { label: nlsHPCC.IsReplicated, type: "checkbox", value: file?.DFUFilePartsOnClusters?.DFUFilePartsOnCluster?.length > 0, readonly: true },
                        "NumParts": { label: nlsHPCC.FileParts, type: "number", value: file?.NumParts, readonly: true },
                        "MinSkew": { label: nlsHPCC.MinSkew, type: "string", value: file?.Stat?.MinSkew, readonly: true },
                        "MaxSkew": { label: nlsHPCC.MaxSkew, type: "string", value: file?.Stat?.MaxSkew, readonly: true },
                        "MinSkewPart": { label: nlsHPCC.MinSkewPart, type: "string", value: file?.Stat?.MinSkewPart === undefined ? "" : file?.Stat?.MinSkewPart.toString(), readonly: true },
                        "MaxSkewPart": { label: nlsHPCC.MaxSkewPart, type: "string", value: file?.Stat?.MaxSkewPart === undefined ? "" : file?.Stat?.MaxSkewPart.toString(), readonly: true },
                    }} onChange={(id, value) => {
                        switch (id) {
                            case "Description":
                                setDescription(value);
                                break;
                            case "isProtected":
                                setProtected(value);
                                break;
                            case "isRestricted":
                                setRestricted(value);
                                break;
                        }
                    }} />
                </ScrollablePane>
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Contents} itemKey="Contents" style={pivotItemStyle(size, 0)}>
            </PivotItem>
            <PivotItem headerText={nlsHPCC.DataPatterns} itemKey="DataPatterns" style={pivotItemStyle(size, 0)}>
                <ScrollablePane scrollbarVisibility={ScrollbarVisibility.auto}>
                    <DataPatternsReport Wuid={file?.Wuid} />
                </ScrollablePane>
            </PivotItem>
            <PivotItem headerText={nlsHPCC.ECL} itemKey="ECL" style={pivotItemStyle(size, 0)}>
                <ECLSourceEditor text={file?.Ecl} readonly={true} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.DEF} itemKey="DEF" style={pivotItemStyle(size, 0)}>
                <XMLSourceEditor text={defFile} readonly={true} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.XML} itemKey="XML" style={pivotItemStyle(size, 0)}>
                <XMLSourceEditor text={xmlFile} readonly={true} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Superfiles} itemKey="superfiles" itemCount={file?.Superfiles?.DFULogicalFile.length || 0} style={pivotItemStyle(size, 0)}>
                <SuperFiles cluster={cluster} logicalFile={logicalFile} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.FileParts} itemKey="FileParts" style={pivotItemStyle(size, 0)}>
                <FileParts cluster={cluster} logicalFile={logicalFile} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Queries} itemIcon="Search" itemKey="queries" style={pivotItemStyle(size, 0)}>
                <Queries filter={{ FileName: logicalFile }} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Queries} itemKey="Queries" style={pivotItemStyle(size, 0)}>
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Graphs} itemKey="Graphs" itemCount={file?.Graphs?.ECLGraph?.length} headerButtonProps={{ disabled: isDFUWorkunit }} style={pivotItemStyle(size, 0)}>
                <FileDetailsGraph cluster={cluster} logicalFile={logicalFile} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Workunit} itemKey="Workunit" style={pivotItemStyle(size, 0)}>
                {isDFUWorkunit ? <DojoAdapter widgetClassID="DFUWUDetailsWidget" params={{ Wuid: file?.Wuid }} /> : <WorkunitDetails wuid={file?.Wuid} />}
            </PivotItem>
            <PivotItem headerText={nlsHPCC.History} itemKey="History" style={pivotItemStyle(size, 0)}>
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Blooms} itemKey="Blooms" itemCount={file?.Blooms?.DFUFileBloom?.length} style={pivotItemStyle(size, 0)}>
                <FileBlooms cluster={cluster} logicalFile={logicalFile} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.ProtectBy} itemKey="ProtectBy" style={pivotItemStyle(size, 0)}>
            </PivotItem>
        </Pivot>
    }</SizeMe>;
};
