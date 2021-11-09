import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Image, Link, ScrollablePane, Sticky } from "@fluentui/react";
import * as ESPLogicalFile from "src/ESPLogicalFile";
import * as WsDfu from "src/WsDfu";
import * as Utility from "src/Utility";
import { useConfirm } from "../hooks/confirm";
import { useFile } from "../hooks/file";
import { useFluentGrid2 } from "../hooks/grid";
import { ShortVerticalDivider } from "./Common";
import { selector } from "./DojoGrid";
import { pushUrl } from "../util/history";
import nlsHPCC from "src/nlsHPCC";

const defaultUIState = {
    hasSelection: false,
};

interface SubFilesProps {
    cluster?: string;
    logicalFile: string;
}

export const SubFiles: React.FunctionComponent<SubFilesProps> = ({
    cluster,
    logicalFile
}) => {

    const [file, , , refresh] = useFile(cluster, logicalFile);
    const [selectedFileList, setSelectedFileList] = React.useState("");
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const [Grid, selection, copyButtons] = useFluentGrid2({
        data,
        primaryID: "Name",
        alphaNumColumns: { RecordCount: true, Totalsize: true },
        query: {},
        sort: [{ attribute: "Modified", "descending": true }],
        filename: "subfiles",
        columns: {
            sel: selector({ width: 27, selectorType: "checkbox" }),
            IsCompressed: {
                width: 25, sortable: false,
                renderHeaderCell: function (node) {
                    node.innerHTML = Utility.getImageHTML("compressed.png", nlsHPCC.Compressed);
                },
                formatter: function (compressed) {
                    if (compressed === true) {
                        return <Image src={Utility.getImageURL("compressed.png")} />;
                    }
                    return <></>;
                }
            },
            IsKeyFile: {
                width: 25, sortable: false,
                renderHeaderCell: function (node) {
                    node.innerHTML = Utility.getImageHTML("index.png", nlsHPCC.Index);
                },
                formatter: function (keyfile, row) {
                    if (row.ContentType === "key") {
                        return <Image src={Utility.getImageURL("index.png")} />;
                    }
                    return <></>;
                }
            },
            isSuperfile: {
                width: 25, sortable: false,
                renderHeaderCell: function (node) {
                    node.innerHTML = Utility.getImageHTML("superfile.png", nlsHPCC.Superfile);
                },
                formatter: function (superfile) {
                    if (superfile === true) {
                        return <Image src={Utility.getImageURL("superfile.png")} />;
                    }
                    return <></>;
                }
            },
            Name: {
                label: nlsHPCC.LogicalName,
                formatter: function (name, row) {
                    const url = "#/files/" + (row.NodeGroup ? row.NodeGroup + "/" : "") + name;
                    return <Link href={url}>{name}</Link>;
                }
            },
            Owner: { label: nlsHPCC.Owner, width: 72 },
            Description: { label: nlsHPCC.Description, width: 153 },
            RecordCount: {
                label: nlsHPCC.Records, width: 72, sortable: false,
                formatter: function (recordCount, row) {
                    return Utility.valueCleanUp(recordCount);
                },
            },
            Totalsize: {
                label: nlsHPCC.Size, width: 72, sortable: false,
                formatter: function (totalSize, row) {
                    return Utility.valueCleanUp(totalSize);
                },
            },
            Parts: {
                label: nlsHPCC.Parts, width: 45, sortable: false,
            },
            Modified: { label: nlsHPCC.ModifiedUTCGMT, width: 155, sortable: false }
        }
    });

    const [DeleteSubfilesConfirm, setShowDeleteSubfilesConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.RemoveSubfiles2 + "\n\n" + selectedFileList,
        onSubmit: React.useCallback(() => {
            WsDfu.SuperfileAction("remove", file.Name, selection, false).then(() => refresh());
        }, [file, refresh, selection])
    });

    React.useEffect(() => {
        const subfiles = [];
        const promises = [];

        file?.subfiles?.Item.forEach(item => {
            const logicalFile = ESPLogicalFile.Get("", item);
            promises.push(logicalFile.getInfo2({
                onAfterSend: function (response) {
                }
            }));
            subfiles.push(logicalFile);
        });
        Promise.all(promises).then(logicalFiles => {
            setData(subfiles);
        });
    }, [file?.subfiles]);

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "open", text: nlsHPCC.Open,
            onClick: () => {
                if (selection.length === 1) {
                    pushUrl("#/files/" + (selection[0].NodeGroup ? selection[0].NodeGroup + "/" : "") + selection[0].Name);
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open("#/files/" + (selection[i].NodeGroup ? selection[i].NodeGroup + "/" : "") + selection[i].Name, "_blank");
                    }
                }
            }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "delete", text: nlsHPCC.RemoveSubfiles, iconProps: { iconName: "Delete" }, disabled: !uiState.hasSelection,
            onClick: () => setShowDeleteSubfilesConfirm(true)
        },
    ], [selection, setShowDeleteSubfilesConfirm, uiState.hasSelection]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        for (let i = 0; i < selection.length; i++) {
            state.hasSelection = true;
        }
        setSelectedFileList(selection.map(s => s.Name).join("\n"));
        setUIState(state);
    }, [selection]);

    return <>
        <ScrollablePane>
            <Sticky>
                <CommandBar items={buttons} farItems={copyButtons} />
            </Sticky>
            <Grid />
        </ScrollablePane>
        <DeleteSubfilesConfirm />
    </>;
};