import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, MessageBar, MessageBarType, ScrollablePane, ScrollbarVisibility, Sticky, StickyPositionType } from "@fluentui/react";
import { format as d3Format } from "@hpcc-js/common";
import { DFUService, WsDfu } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { formatCost } from "src/Session";
import * as Utility from "src/Utility";
import { getStateImageName, IFile } from "src/ESPLogicalFile";
import { useConfirm } from "../hooks/confirm";
import { useFile } from "../hooks/file";
import { ShortVerticalDivider } from "./Common";
import { TableGroup } from "./forms/Groups";
import { CopyFile } from "./forms/CopyFile";
import { DesprayFile } from "./forms/DesprayFile";
import { RenameFile } from "./forms/RenameFile";
import { ReplicateFile } from "./forms/ReplicateFile";
import { replaceUrl } from "../util/history";

const logger = scopedLogger("src-react/components/LogicalFileSummary.tsx");

import "react-reflex/styles.css";

const dfuService = new DFUService({ baseUrl: "" });

interface LogicalFileSummaryProps {
    cluster?: string;
    logicalFile: string;
    tab?: string;
}

const formatInt = d3Format(",");

export const LogicalFileSummary: React.FunctionComponent<LogicalFileSummaryProps> = ({
    cluster,
    logicalFile,
    tab = "summary"
}) => {

    const [file, isProtected, , refresh] = useFile(cluster, logicalFile);
    const [description, setDescription] = React.useState("");
    const [_protected, setProtected] = React.useState(false);
    const [restricted, setRestricted] = React.useState(false);
    const [canReplicateFlag, setCanReplicateFlag] = React.useState(false);
    const [replicateFlag, setReplicateFlag] = React.useState(false);
    const [showCopyFile, setShowCopyFile] = React.useState(false);
    const [showRenameFile, setShowRenameFile] = React.useState(false);
    const [showDesprayFile, setShowDesprayFile] = React.useState(false);
    const [showReplicateFile, setShowReplicateFile] = React.useState(false);

    const [showMessageBar, setShowMessageBar] = React.useState(false);
    const dismissMessageBar = React.useCallback(() => setShowMessageBar(false), []);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.YouAreAboutToDeleteThisFile,
        onSubmit: React.useCallback(() => {
            dfuService.DFUArrayAction({ Type: WsDfu.DFUArrayActions.Delete, LogicalFiles: { Item: [file.Name] } }).then(({ ActionResults }) => {
                const actionInfo = ActionResults?.DFUActionInfo;
                if (actionInfo && actionInfo.length && !actionInfo[0].Failed) {
                    replaceUrl("/files");
                } else {
                    logger.error(actionInfo[0].ActionResult);
                }
            }).catch(err => logger.error(err));
        }, [file])
    });

    const isDFUWorkunit = React.useMemo(() => {
        return file?.Wuid?.length && file?.Wuid[0] === "D";
    }, [file?.Wuid]);

    React.useEffect(() => {
        setDescription(file?.Description || "");
        setProtected(file?.ProtectList?.DFUFileProtect?.length > 0 || false);
        setRestricted(file?.IsRestricted || false);

        if ((file?.filePartsOnCluster() ?? []).length > 0) {
            let _canReplicate = false;
            let _replicate = false;
            file?.filePartsOnCluster().forEach(part => {
                _canReplicate = _canReplicate && part.CanReplicate;
                _replicate = _replicate && part.Replicate;
            });
            setCanReplicateFlag(_canReplicate);
            setReplicateFlag(_replicate);
        }

    }, [file]);

    const canSave = React.useMemo(() => {
        return file && (
            description !== file?.Description ||
            _protected !== isProtected ||
            restricted !== file?.IsRestricted
        );
    }, [_protected, description, file, isProtected, restricted]);

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => {
                refresh();
            }
        },
        {
            key: "copyFilename", text: nlsHPCC.CopyLogicalFilename, iconProps: { iconName: "Copy" },
            onClick: () => {
                navigator?.clipboard?.writeText(logicalFile);
            }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "save", text: nlsHPCC.Save, iconProps: { iconName: "Save" }, disabled: !canSave,
            onClick: () => {
                file?.update({
                    UpdateDescription: true,
                    FileDesc: description,
                    Protect: _protected ? WsDfu.DFUChangeProtection.Protect : WsDfu.DFUChangeProtection.Unprotect,
                    Restrict: restricted ? WsDfu.DFUChangeRestriction.Restrict : WsDfu.DFUChangeRestriction.Unrestricted,
                })
                    .then(_ => {
                        setShowMessageBar(true);
                        const t = window.setTimeout(function () {
                            setShowMessageBar(false);
                            window.clearTimeout(t);
                        }, 2400);
                    })
                    .catch(err => logger.error(err));
            }
        },
        {
            key: "delete", text: nlsHPCC.Delete, iconProps: { iconName: "Delete" }, disabled: !file,
            onClick: () => setShowDeleteConfirm(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "copyFile", text: nlsHPCC.Copy, disabled: !file,
            onClick: () => setShowCopyFile(true)
        },
        {
            key: "rename", text: nlsHPCC.Rename, disabled: !file,
            onClick: () => setShowRenameFile(true)
        },
        {
            key: "despray", text: nlsHPCC.Despray, disabled: !file,
            onClick: () => setShowDesprayFile(true)
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "replicate", text: nlsHPCC.Replicate, disabled: !canReplicateFlag || !replicateFlag,
            onClick: () => setShowReplicateFile(true)
        },
    ], [_protected, canReplicateFlag, canSave, description, file, logicalFile, refresh, replicateFlag, restricted, setShowDeleteConfirm]);

    const protectedImage = _protected ? Utility.getImageURL("locked.png") : Utility.getImageURL("unlocked.png");
    const stateImage = Utility.getImageURL(getStateImageName(file as unknown as IFile));
    const compressedImage = file?.IsCompressed ? Utility.getImageURL("compressed.png") : "";

    return <>
        <ScrollablePane scrollbarVisibility={ScrollbarVisibility.auto}>
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
                "SuperOwner": { label: nlsHPCC.SuperFile, type: "links", links: file?.Superfiles?.DFULogicalFile?.map(row => ({ label: "", type: "link", value: row.Name, href: `#/files/${row.Name}` })) },
                "NodeGroup": { label: nlsHPCC.ClusterName, type: "string", value: file?.NodeGroup, readonly: true },
                "Description": { label: nlsHPCC.Description, type: "string", value: description },
                "JobName": { label: nlsHPCC.JobName, type: "string", value: file?.JobName, readonly: true },
                "AccessCost": { label: nlsHPCC.FileAccessCost, type: "string", value: `${formatCost(file?.AccessCost)}`, readonly: true },
                "AtRestCost": { label: nlsHPCC.FileCostAtRest, type: "string", value: `${formatCost(file?.AtRestCost)}`, readonly: true },
                "isProtected": { label: nlsHPCC.Protected, type: "checkbox", value: _protected },
                "isRestricted": { label: nlsHPCC.Restricted, type: "checkbox", value: restricted },
                "ContentType": { label: nlsHPCC.ContentType, type: "string", value: file?.ContentType, readonly: true },
                "KeyType": { label: nlsHPCC.KeyType, type: "string", value: file?.KeyType, readonly: true },
                "Format": { label: nlsHPCC.Format, type: "string", value: file?.Format, readonly: true },
                "IsCompressed": { label: nlsHPCC.IsCompressed, type: "checkbox", value: file?.IsCompressed, readonly: true },
                "CompressedFileSizeString": { label: nlsHPCC.CompressedFileSize, type: "string", value: file?.CompressedFileSize ? formatInt(file?.CompressedFileSize) : "", readonly: true },
                "Filesize": { label: nlsHPCC.FileSize, type: "string", value: file?.Filesize, readonly: true },
                "PercentCompressed": { label: nlsHPCC.PercentCompressed, type: "string", value: file?.PercentCompressed, readonly: true },
                "Modified": { label: nlsHPCC.Modified, type: "string", value: file?.Modified, readonly: true },
                "ExpirationDate": { label: nlsHPCC.ExpirationDate, type: "string", value: file?.ExpirationDate, readonly: true },
                "ExpireDays": { label: nlsHPCC.ExpireDays, type: "string", value: file?.ExpireDays ? file?.ExpireDays.toString() : "", readonly: true },
                "Directory": { label: nlsHPCC.Directory, type: "string", value: file?.Dir, readonly: true },
                "PathMask": { label: nlsHPCC.PathMask, type: "string", value: file?.PathMask, readonly: true },
                "RecordSize": { label: nlsHPCC.RecordSize, type: "string", value: file?.RecordSize, readonly: true },
                "RecordCount": { label: nlsHPCC.RecordCount, type: "string", value: file?.RecordCount, readonly: true },
                "IsReplicated": { label: nlsHPCC.IsReplicated, type: "checkbox", value: (file?.filePartsOnCluster() ?? []).length > 0, readonly: true },
                "NumParts": { label: nlsHPCC.FileParts, type: "number", value: file?.NumParts, readonly: true },
                "MinSkew": { label: nlsHPCC.MinSkew, type: "string", value: `${Utility.formatDecimal(file?.Stat?.MinSkewInt64 / 100 ?? 0)}%`, readonly: true },
                "MaxSkew": { label: nlsHPCC.MaxSkew, type: "string", value: `${Utility.formatDecimal(file?.Stat?.MaxSkewInt64 / 100 ?? 0)}%`, readonly: true },
                "MinSkewPart": { label: nlsHPCC.MinSkewPart, type: "string", value: file?.Stat?.MinSkewPart === undefined ? "" : file?.Stat?.MinSkewPart?.toString(), readonly: true },
                "MaxSkewPart": { label: nlsHPCC.MaxSkewPart, type: "string", value: file?.Stat?.MaxSkewPart === undefined ? "" : file?.Stat?.MaxSkewPart?.toString(), readonly: true },
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
        <CopyFile logicalFiles={[logicalFile]} showForm={showCopyFile} setShowForm={setShowCopyFile} />
        <DesprayFile logicalFiles={[logicalFile]} showForm={showDesprayFile} setShowForm={setShowDesprayFile} />
        <RenameFile logicalFiles={[logicalFile]} showForm={showRenameFile} setShowForm={setShowRenameFile} />
        <ReplicateFile cluster={cluster} logicalFile={logicalFile} showForm={showReplicateFile} setShowForm={setShowReplicateFile} />
        <DeleteConfirm />
    </>;
};
