import * as React from "react";
import { Button, Card, makeStyles, MessageBar, MessageBarActions, MessageBarBody, tokens } from "@fluentui/react-components";
import { CopyRegular, DismissRegular, DocumentRegular, FolderZipRegular, LockClosedRegular, LockOpenRegular } from "@fluentui/react-icons";
import { DFUService, WsDfu } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { ScrollablePane, ScrollbarVisibility } from "./controls/ScrollablePane";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "./CommandBarV9";
import nlsHPCC from "src/nlsHPCC";
import { formatCost } from "src/Session";
import { copyToClipboard, safeFormatNum } from "src/Utility";
import { useConfirm } from "../hooks/confirm";
import { useFile } from "../hooks/file";
import { useMyAccount } from "../hooks/user";
import { TableGroup } from "./forms/Groups";
import { CopyFile } from "./forms/CopyFile";
import { DesprayFile } from "./forms/DesprayFile";
import { RenameFile } from "./forms/RenameFile";
import { ReplicateFile } from "./forms/ReplicateFile";
import { HolyGrail } from "../layouts/HolyGrail";
import { DockPanel, DockPanelItem } from "../layouts/DockPanel";
import { replaceUrl } from "../util/history";

const logger = scopedLogger("src-react/components/LogicalFileSummary.tsx");

const dfuService = new DFUService({ baseUrl: "" });

const useStyles = makeStyles({
    fileSummaryWrapper: {
        containerType: "inline-size"
    },
    fileSummaryHeader: {
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
        containerType: "inline-size",
        "& h2": {
            margin: "4px 0 10px 0",
            display: "flex",
            alignItems: "center"
        }
    },
    copyButton: {
        minWidth: "24px",
        maxWidth: "24px",
        height: "23px",
        margin: "0 0 0 6px",
        "& .fui-Button__icon": {
            height: "16px",
            width: "16px"
        }
    },
    cardsWrapper: {
        display: "grid",
        gridTemplateColumns: "2fr 2fr",
        gap: "12px",
        margin: "0 4px 4px 4px",
        containerType: "inline-size",
        "@container (max-width: 700px)": {
            gridTemplateColumns: "1fr"
        }
    },
    detailsPanel: {
        overflowX: "auto",
        "& a": {
            fontSize: tokens.fontSizeBase300
        }
    },
    rightCardsGrid: {
        display: "grid",
        gridTemplateColumns: "1fr 1fr",
        gridTemplateRows: "1fr 1fr",
        gap: "12px",
        "@container (max-width: 1100px)": {
            gridTemplateColumns: "1fr",
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

interface LogicalFileSummaryProps {
    cluster?: string;
    logicalFile: string;
    tab?: string;
}

export const LogicalFileSummary: React.FunctionComponent<LogicalFileSummaryProps> = ({
    cluster,
    logicalFile,
    tab = "summary"
}) => {

    const { file, isProtected, protectedBy, superfiles, refreshData } = useFile(cluster, logicalFile);
    const { currentUser } = useMyAccount();
    const [description, setDescription] = React.useState("");
    const [_protected, setProtected] = React.useState(false);
    const [restricted, setRestricted] = React.useState(false);
    const [canReplicateFlag, setCanReplicateFlag] = React.useState(false);
    const [replicateFlag, setReplicateFlag] = React.useState(false);
    const [showCopyFile, setShowCopyFile] = React.useState(false);
    const [showRenameFile, setShowRenameFile] = React.useState(false);
    const [showDesprayFile, setShowDesprayFile] = React.useState(false);
    const [showReplicateFile, setShowReplicateFile] = React.useState(false);

    const styles = useStyles();

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
        return file?.Wuid?.length && (file?.Wuid[0] === "D" || file?.Wuid[0] === "P");
    }, [file?.Wuid]);

    React.useEffect(() => {
        setDescription(file?.Description || "");
        setProtected(isProtected);
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

    }, [file, isProtected]);

    const canSave = React.useMemo(() => {
        return file && (
            description !== file?.Description ||
            _protected !== isProtected ||
            restricted !== file?.IsRestricted
        );
    }, [_protected, description, file, isProtected, restricted]);

    const protectedByCurrentUser = React.useMemo(() => {
        if (currentUser.username) {
            return protectedBy.filter(p => p.Owner === currentUser.username).length > 0;
        } else {
            return _protected;
        }
    }, [currentUser, _protected, protectedBy]);

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider },
        {
            key: "save", text: nlsHPCC.Save, iconProps: { iconName: "Save" }, disabled: !canSave,
            onClick: () => {
                file?.update({
                    UpdateDescription: true,
                    FileDesc: description
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
        { key: "divider_2", itemType: ContextualMenuItemType.Divider },
        {
            key: "protect", text: nlsHPCC.Protect, iconProps: { iconName: "Lock" }, disabled: protectedByCurrentUser,
            onClick: () => {
                file?.update({ Protect: WsDfu.DFUChangeProtection.Protect })
                    .then(() => {
                        setProtected(true);
                        refreshData();
                    })
                    .catch(err => logger.error(err));
            }
        },
        {
            key: "unprotect", text: nlsHPCC.Unprotect, iconProps: { iconName: "Unlock" }, disabled: !protectedByCurrentUser,
            onClick: () => {
                file?.update({ Protect: WsDfu.DFUChangeProtection.Unprotect })
                    .then(() => {
                        setProtected(false);
                        refreshData();
                    })
                    .catch(err => logger.error(err));
            }
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider },
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
        { key: "divider_4", itemType: ContextualMenuItemType.Divider },
        {
            key: "replicate", text: nlsHPCC.Replicate, disabled: !canReplicateFlag || !replicateFlag,
            onClick: () => setShowReplicateFile(true)
        },
    ], [canReplicateFlag, canSave, description, file, protectedByCurrentUser, refreshData, replicateFlag, setShowDeleteConfirm]);

    return <HolyGrail
        header={<>
            <CommandBar items={buttons} />
            {showMessageBar &&
                <MessageBar intent="success">
                    <MessageBarBody>{nlsHPCC.SuccessfullySaved}</MessageBarBody>
                    <MessageBarActions containerAction={<Button onClick={dismissMessageBar} aria-label={nlsHPCC.Close} appearance="transparent" icon={<DismissRegular />} />} />
                </MessageBar>
            }
        </>
        }
        main={<>
            <DockPanel hideSingleTabs>
                <DockPanelItem key="summary" title="Summary">
                    <ScrollablePane scrollbarVisibility={ScrollbarVisibility.auto}>
                        <div className={`${styles.fileSummaryWrapper} pane-content`}>
                            <div className={styles.fileSummaryHeader}>
                                <h2>
                                    <FolderZipRegular style={{ marginLeft: 6, color: !file?.IsCompressed ? tokens.colorNeutralForegroundDisabled : "inherit" }} />
                                    {isProtected ? <LockClosedRegular /> : <LockOpenRegular style={{ margin: "0 4px", color: tokens.colorNeutralForegroundDisabled }} />}
                                    <DocumentRegular style={{ margin: "2px 6px 0 0" }} />
                                    {file?.Name}
                                    <Button title={nlsHPCC.CopyLogicalFilename} aria-label={nlsHPCC.CopyLogicalFilename} className={styles.copyButton} icon={<CopyRegular />}
                                        onClick={() => copyToClipboard(file?.Name)}
                                    />
                                </h2>
                            </div>
                            <div className={styles.cardsWrapper}>
                                <Card className={styles.detailsPanel}>
                                    <TableGroup fields={{
                                        "Wuid": { label: nlsHPCC.Workunit, type: "link", value: file?.Wuid, href: `#/${isDFUWorkunit ? "dfu" : ""}workunits/${file?.Wuid}`, readonly: true, onCopy: () => copyToClipboard(file?.Wuid) },
                                        "Owner": { label: nlsHPCC.Owner, type: "link", value: file?.Owner, title: nlsHPCC.ViewFilesByOwner, href: file?.Owner ? `#/files?Owner=${encodeURIComponent(file?.Owner ?? "")}` : "", readonly: true, onCopy: () => copyToClipboard(file?.Owner) },
                                        "SuperOwner": { label: nlsHPCC.SuperFile, type: "links", links: superfiles?.map(row => ({ label: "", type: "link", value: row.Name, href: `#/files/${row.NodeGroup !== null ? row.NodeGroup : undefined}/${row.Name}` })) },
                                        "NodeGroup": { label: nlsHPCC.ClusterName, type: "string", value: file?.NodeGroup, readonly: true },
                                        "Description": { label: nlsHPCC.Description, type: "string", value: description },
                                        "JobName": { label: nlsHPCC.JobName, type: "link", value: file?.JobName, title: nlsHPCC.ViewWUsWithSimilarName, href: file?.JobName ? `#/${isDFUWorkunit ? "dfu" : ""}workunits/?Jobname=*${file?.JobName}*` : "", readonly: true, onCopy: () => copyToClipboard(file?.JobName) },
                                        "isRestricted": { label: nlsHPCC.Restricted, type: "checkbox", value: restricted },
                                        "ContentType": { label: nlsHPCC.ContentType, type: "string", value: file?.ContentType, readonly: true },
                                        "KeyType": { label: nlsHPCC.KeyType, type: "string", value: file?.KeyType, readonly: true },
                                        "Format": { label: nlsHPCC.Format, type: "string", value: file?.Format, readonly: true },
                                        "Modified": { label: nlsHPCC.Modified, type: "string", value: file?.Modified, readonly: true },
                                        "ExpirationDate": { label: nlsHPCC.ExpirationDate, type: "string", value: file?.ExpirationDate, readonly: true },
                                        "ExpireDays": { label: nlsHPCC.ExpireDays, type: "string", value: file?.ExpireDays ? file?.ExpireDays.toString() : "", readonly: true },
                                    }} onChange={(id, value) => {
                                        switch (id) {
                                            case "Description":
                                                setDescription(value);
                                                break;
                                            case "isProtected":
                                                setProtected(value);
                                                file?.update({
                                                    Protect: value ? WsDfu.DFUChangeProtection.Protect : WsDfu.DFUChangeProtection.Unprotect,
                                                }).then(() => {
                                                    refreshData();
                                                }).catch(err => logger.error(err));
                                                break;
                                            case "isRestricted":
                                                setRestricted(value);
                                                file?.update({
                                                    Restrict: value ? WsDfu.DFUChangeRestriction.Restrict : WsDfu.DFUChangeRestriction.Unrestricted,
                                                }).catch(err => logger.error(err));
                                                break;
                                        }
                                    }} />
                                </Card>
                                <div className={styles.rightCardsGrid}>
                                    <Card>
                                        <TableGroup fields={{
                                            "AccessCost": { label: nlsHPCC.FileAccessCost, type: "string", value: `${formatCost(file?.AccessCost)}`, readonly: true },
                                            "AtRestCost": { label: nlsHPCC.FileCostAtRest, type: "string", value: `${formatCost(file?.AtRestCost)}`, readonly: true },
                                            "MinSkew": { label: nlsHPCC.MinSkew, type: "string", value: file?.Stat?.MinSkew ? `${file.Stat.MinSkew}%` : "", readonly: true },
                                            "MaxSkew": { label: nlsHPCC.MaxSkew, type: "string", value: file?.Stat?.MaxSkew ? `${file.Stat.MaxSkew}%` : "", readonly: true },
                                            "MinSkewPart": { label: nlsHPCC.MinSkewPart, type: "string", value: file?.Stat?.MinSkewPart === undefined ? "" : file?.Stat?.MinSkewPart?.toString(), readonly: true },
                                            "MaxSkewPart": { label: nlsHPCC.MaxSkewPart, type: "string", value: file?.Stat?.MaxSkewPart === undefined ? "" : file?.Stat?.MaxSkewPart?.toString(), readonly: true },
                                        }} />
                                    </Card>
                                    <Card>
                                        <TableGroup fields={{
                                            "Directory": { label: nlsHPCC.Directory, type: "string", value: file?.Dir, readonly: true },
                                            "PathMask": { label: nlsHPCC.PathMask, type: "string", value: file?.PathMask, readonly: true },
                                            "RecordSize": { label: nlsHPCC.RecordSize, type: "string", value: file?.RecordSize, readonly: true },
                                            "RecordCount": { label: nlsHPCC.RecordCount, type: "string", value: file?.RecordCount, readonly: true },
                                            "IsReplicated": { label: nlsHPCC.IsReplicated, type: "checkbox", value: (file?.filePartsOnCluster() ?? []).length > 0, readonly: true },
                                            "NumParts": { label: nlsHPCC.FileParts, type: "number", value: file?.NumParts, readonly: true },
                                        }} />
                                    </Card>
                                    <Card>
                                        <TableGroup fields={{
                                            "IsCompressed": { label: nlsHPCC.IsCompressed, type: "checkbox", value: file?.IsCompressed, readonly: true },
                                            "CompressedFileSizeString": { label: nlsHPCC.CompressedFileSize, type: "string", value: file?.CompressedFileSize ? safeFormatNum(file?.CompressedFileSize) : "", readonly: true },
                                            "CompressionType": { label: nlsHPCC.CompressionType, type: "string", value: file?.CompressionType, readonly: true },
                                            "Filesize": { label: nlsHPCC.FileSize, type: "string", value: file?.Filesize, readonly: true },
                                            "PercentCompressed": { label: nlsHPCC.PercentCompressed, type: "string", value: file?.PercentCompressed, readonly: true },
                                        }} />
                                    </Card>
                                </div>

                            </div>
                        </div>
                    </ScrollablePane>
                </DockPanelItem>
            </DockPanel>

            <CopyFile logicalFiles={[logicalFile]} showForm={showCopyFile} setShowForm={setShowCopyFile} />
            <DesprayFile logicalFiles={[logicalFile]} showForm={showDesprayFile} setShowForm={setShowDesprayFile} />
            <RenameFile logicalFiles={[logicalFile]} showForm={showRenameFile} setShowForm={setShowRenameFile} />
            <ReplicateFile cluster={cluster} logicalFile={logicalFile} showForm={showReplicateFile} setShowForm={setShowReplicateFile} />
            <DeleteConfirm />
        </>
        }
    />;
};
