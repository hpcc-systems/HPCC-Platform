import * as React from "react";
import { Button, makeStyles, MessageBar, MessageBarActions, MessageBarBody, tokens } from "@fluentui/react-components";
import { CopyRegular, DismissRegular, FolderZipRegular, ListRegular, LockClosedRegular, LockOpenRegular } from "@fluentui/react-icons";
import { DFUService, WsDfu } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { ScrollablePane, ScrollbarVisibility } from "./controls/ScrollablePane";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "./CommandBarV9";
import { SubFiles } from "./SubFiles";
import nlsHPCC from "src/nlsHPCC";
import { copyToClipboard } from "src/Utility";
import { useConfirm } from "../hooks/confirm";
import { useFile } from "../hooks/file";
import { TableGroup } from "./forms/Groups";
import { CopyFile } from "./forms/CopyFile";
import { HolyGrail } from "../layouts/HolyGrail";
import { DockPanel, DockPanelItem } from "../layouts/DockPanel";
import { replaceUrl } from "../util/history";

const logger = scopedLogger("src-react/components/SuperFileSummary.tsx");

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

interface SuperFileSummaryProps {
    cluster?: string;
    logicalFile: string;
}

export const SuperFileSummary: React.FunctionComponent<SuperFileSummaryProps> = ({
    cluster,
    logicalFile
}) => {

    const { file, isProtected, refreshData } = useFile(cluster, logicalFile);
    const [description, setDescription] = React.useState("");
    const [_protected, setProtected] = React.useState(false);
    const [restricted, setRestricted] = React.useState(false);
    const [showCopyFile, setShowCopyFile] = React.useState(false);

    const styles = useStyles();

    const [showMessageBar, setShowMessageBar] = React.useState(false);
    const dismissMessageBar = React.useCallback(() => setShowMessageBar(false), []);

    React.useEffect(() => {
        setDescription(file?.Description || "");
        setProtected(isProtected);
        setRestricted(file?.IsRestricted || false);
    }, [file, isProtected]);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSuperfile,
        onSubmit: React.useCallback(() => {
            if (!file) return;
            const subfiles = (file.subfiles?.Item ?? []).map(sf => ({ Name: sf }));
            dfuService.SuperfileAction({
                action: "remove",
                superfile: file.Name ?? "",
                subfiles: { Item: subfiles.map(file => file.Name) },
                removeSuperfile: true
            })
                .then(() => replaceUrl("/files"))
                .catch(err => logger.error(err))
                ;
        }, [file])
    });

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
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider },
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
                    .catch(err => logger.error(err))
                    ;
            }
        },
        {
            key: "delete", text: nlsHPCC.DeleteSuperfile, iconProps: { iconName: "Delete" }, disabled: !file,
            onClick: () => setShowDeleteConfirm(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider },
        {
            key: "copyFile", text: nlsHPCC.Copy, disabled: !file,
            onClick: () => setShowCopyFile(true)
        }
    ], [_protected, canSave, description, file, refreshData, restricted, setShowDeleteConfirm]);

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
                                    <ListRegular style={{ margin: "2px 6px 0 0" }} />
                                    {file?.Name}
                                    <Button title={nlsHPCC.CopyLogicalFilename} aria-label={nlsHPCC.CopyLogicalFilename} className={styles.copyButton} icon={<CopyRegular />}
                                        onClick={() => copyToClipboard(file?.Name)}
                                    />
                                </h2>
                            </div>
                            <div className={styles.cardsWrapper}>
                                <div className={styles.detailsPanel}>
                                    <TableGroup fields={{
                                        "Owner": { label: nlsHPCC.Owner, type: "link", value: file?.Owner, title: nlsHPCC.ViewFilesByOwner, href: file?.Owner ? `#/files?Owner=${encodeURIComponent(file?.Owner ?? "")}` : "", readonly: true, onCopy: () => copyToClipboard(file?.Owner) },
                                        "Description": { label: nlsHPCC.Description, type: "string", value: description, multiline: true },
                                        "JobName": { label: nlsHPCC.JobName, type: "link", value: file?.JobName, title: nlsHPCC.ViewWUsWithSimilarName, href: file?.JobName ? `#/workunits/?Jobname=*${file?.JobName}*` : "", readonly: true, onCopy: () => copyToClipboard(file?.JobName) },
                                        "Filesize": { label: nlsHPCC.FileSize, type: "string", value: file?.Filesize, readonly: true },
                                        "isProtected": { label: nlsHPCC.Protected, type: "checkbox", value: _protected },
                                        "IsCompressed": { label: nlsHPCC.IsCompressed, type: "checkbox", value: file?.IsCompressed, readonly: true },
                                        "PercentCompressed": { label: nlsHPCC.PercentCompressed, type: "string", value: file?.PercentCompressed, readonly: true },
                                    }} onChange={(id, value) => {
                                        switch (id) {
                                            case "Description":
                                                setDescription(value);
                                                break;
                                            case "isProtected":
                                                setProtected(value);
                                                file?.update({
                                                    Protect: value ? WsDfu.DFUChangeProtection.Protect : WsDfu.DFUChangeProtection.Unprotect,
                                                }).catch(err => logger.error(err));
                                                break;
                                        }
                                    }} />
                                </div>
                            </div>
                        </div>
                    </ScrollablePane>
                </DockPanelItem>
                <DockPanelItem key="errWarn" title="ErrWarn" padding={4} location="split-bottom" relativeTo="helpersTable">
                    <SubFiles cluster={cluster} logicalFile={logicalFile} />
                </DockPanelItem>
            </DockPanel>
            <CopyFile logicalFiles={[logicalFile]} showForm={showCopyFile} setShowForm={setShowCopyFile} />
            <DeleteConfirm />
        </>
        }
    />;
};
