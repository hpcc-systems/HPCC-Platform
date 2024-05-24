import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link, ScrollablePane, Sticky } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { useFile } from "../hooks/file";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { ShortVerticalDivider } from "./Common";

const defaultUIState = {
    hasSelection: false
};

interface SuperFilesProps {
    cluster: string;
    logicalFile: string;
    sort?: QuerySortItem;
}

const defaultSort = { attribute: "Name", descending: false };

export const SuperFiles: React.FunctionComponent<SuperFilesProps> = ({
    cluster,
    logicalFile,
    sort = defaultSort
}) => {

    const [file, , , refreshData] = useFile(cluster, logicalFile);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            col1: {
                width: 27,
                selectorType: "checkbox"
            },
            Name: {
                label: nlsHPCC.Name,
                sortable: true,
                formatter: (name, row) => {
                    return <Link href={`#/files/${row.NodeGroup !== null ? row.NodeGroup : undefined}/${name}`}>{name}</Link>;
                }
            }
        };
    }, []);

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

    const copyButtons = useCopyButtons(columns, selection, "superFiles");

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
        <FluentGrid
            data={data}
            primaryID={"Name"}
            sort={sort}
            columns={columns}
            setSelection={setSelection}
            setTotal={setTotal}
            refresh={refreshTable}
        ></FluentGrid>
    </ScrollablePane>;
};
