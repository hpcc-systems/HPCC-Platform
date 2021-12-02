import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link, ScrollablePane, Sticky } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { useFluentGrid } from "../hooks/grid";
import { useFile } from "../hooks/file";
import { ShortVerticalDivider } from "./Common";
import { selector } from "./DojoGrid";

const defaultUIState = {
    hasSelection: false
};

interface SuperFilesProps {
    cluster: string;
    logicalFile: string;
}

export const SuperFiles: React.FunctionComponent<SuperFilesProps> = ({
    cluster,
    logicalFile
}) => {

    const [file, , , refreshData] = useFile(cluster, logicalFile);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const [Grid, selection, copyButtons] = useFluentGrid({
        data,
        primaryID: "Name",
        sort: [{ attribute: "Name", "descending": false }],
        filename: "superFiles",
        columns: {
            col1: selector({
                width: 27,
                selectorType: "checkbox"
            }),
            Name: {
                label: nlsHPCC.Name,
                sortable: true,
                formatter: function (name, row) {
                    return <Link href={`#/files/${cluster}/${name}`}>{name}</Link>;
                }
            },
        }
    });

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.hasSelection, iconProps: { iconName: "WindowEdit" },
            onClick: () => {
                if (selection.length === 1) {
                    window.location.href = `#/files/${cluster}/${selection[0].Name}`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/files/${cluster}/${selection[i].Name}`, "_blank");
                    }
                }
            }
        },
    ], [cluster, refreshData, selection, uiState.hasSelection]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        if (selection.length) {
            state.hasSelection = true;
        }
        setUIState(state);
    }, [selection]);

    React.useEffect(() => {
        if (file?.Superfiles?.DFULogicalFile) {
            setData(file?.Superfiles?.DFULogicalFile);
        }
    }, [file]);

    return <ScrollablePane>
        <Sticky>
            <CommandBar items={buttons} farItems={copyButtons} />
        </Sticky>
        <Grid />
    </ScrollablePane>;
};
