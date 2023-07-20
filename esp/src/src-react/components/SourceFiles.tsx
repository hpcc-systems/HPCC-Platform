import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Image, Link, ScrollablePane, Sticky } from "@fluentui/react";
import * as domClass from "dojo/dom-class";
import * as Utility from "src/Utility";
import { QuerySortItem } from "src/store/Store";
import nlsHPCC from "src/nlsHPCC";
import { useFluentGrid } from "../hooks/grid";
import { useWorkunitSourceFiles } from "../hooks/workunit";
import { pushParams } from "../util/history";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";
import { ShortVerticalDivider } from "./Common";

const FilterFields: Fields = {
    "Name": { type: "string", label: nlsHPCC.Name, placeholder: nlsHPCC.TargetNamePlaceholder },
};

const defaultUIState = {
    hasSelection: false
};

interface SourceFilesProps {
    wuid: string;
    filter?: { [id: string]: any };
    sort?: QuerySortItem
}

const emptyFilter: { [id: string]: any } = {};
const defaultSort = { attribute: "Name", descending: false };

export const SourceFiles: React.FunctionComponent<SourceFilesProps> = ({
    wuid,
    filter = emptyFilter,
    sort = defaultSort
}) => {

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);

    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [sourceFiles, , , refreshData] = useWorkunitSourceFiles(wuid);
    const [data, setData] = React.useState<any[]>([]);
    const [showFilter, setShowFilter] = React.useState(false);

    //  Grid ---
    const { Grid, selection, copyButtons } = useFluentGrid({
        data,
        primaryID: "Name",
        alphaNumColumns: { Name: true, Value: true },
        sort,
        filename: "sourceFiles",
        columns: {
            col1: {
                width: 27,
                selectorType: "checkbox"
            },
            Name: {
                label: "Name", sortable: true,
                formatter: React.useCallback(function (Name, row) {
                    return <>
                        <Image src={Utility.getImageURL(row.IsSuperFile ? "folder_table.png" : "file.png")} />
                        &nbsp;
                        <Link href={`#/files/${row.FileCluster}/${Name}`}>{Name}</Link>
                    </>;
                }, [])
            },
            FileCluster: { label: nlsHPCC.FileCluster, width: 300, sortable: false },
            Count: {
                label: nlsHPCC.Usage, width: 72, sortable: true,
                renderCell: React.useCallback(function (object, value, node, options) {
                    domClass.add(node, "justify-right");
                    node.innerText = Utility.valueCleanUp(value);
                }, [])
            }
        }
    });

    //  Filter  ---
    const filterFields: Fields = {};
    for (const fieldID in FilterFields) {
        filterFields[fieldID] = { ...FilterFields[fieldID], value: filter[fieldID] };
    }

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
                    window.location.href = `#/files/${selection[0].Name}`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/files/${selection[i].Name}`, "_blank");
                    }
                }
            }
        },
        {
            key: "filter", text: nlsHPCC.Filter, disabled: data?.length == 0, iconProps: { iconName: hasFilter ? "FilterSolid" : "Filter" },
            onClick: () => { setShowFilter(true); }
        },
    ], [data.length, hasFilter, refreshData, selection, uiState.hasSelection]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        for (let i = 0; i < selection.length; ++i) {
            state.hasSelection = true;
            break;
        }
        setUIState(state);
    }, [selection]);

    React.useEffect(() => {
        let files = sourceFiles;
        const name = filter?.Name ?? "";
        if (name) {
            files = files.filter(file => file.Name.match(name.replace(/\*/g, ".*")));
        }
        setData(files);
    }, [filter, sourceFiles]);

    return <ScrollablePane>
        <Sticky>
            <CommandBar items={buttons} farItems={copyButtons} />
        </Sticky>
        <Grid />
        <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
    </ScrollablePane>;
};
