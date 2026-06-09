import * as React from "react";
import { Sticky, StickyPositionType } from "./controls/ScrollablePane";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "./CommandBarV9";
import { DFUService, WsDfu } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import * as Utility from "src/Utility";
import { useConfirm } from "../hooks/confirm";
import { useFile } from "../hooks/file";
import { TableGroup } from "./forms/Groups";
import { CopyFile } from "./forms/CopyFile";
import { replaceUrl } from "../util/history";

const logger = scopedLogger("src-react/components/SuperFileSummary.tsx");

const dfuService = new DFUService({ baseUrl: "" });

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

    const protectedImage = _protected ? Utility.getImageURL("locked.png") : Utility.getImageURL("unlocked.png");
    const compressedImage = file?.IsCompressed ? Utility.getImageURL("compressed.png") : "";

    return <>
        <Sticky stickyPosition={StickyPositionType.Header}>
            <CommandBar items={buttons} />
            <div style={{ display: "inline-block" }}>
                <h2>
                    <img src={compressedImage} />&nbsp;
                    <img src={protectedImage} />&nbsp;
                    {file?.Name}
                </h2>
            </div>
        </Sticky>
        <TableGroup fields={{
            "Description": { label: nlsHPCC.Description, type: "string", value: description, multiline: true },
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
        <CopyFile logicalFiles={[logicalFile]} showForm={showCopyFile} setShowForm={setShowCopyFile} />
        <DeleteConfirm />
    </>;
};
