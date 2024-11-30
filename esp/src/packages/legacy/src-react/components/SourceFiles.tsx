import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Image, Link } from "@fluentui/react";
import * as Utility from "src/Utility";
import { QuerySortItem } from "src/store/Store";
import nlsHPCC from "src/nlsHPCC";
import { useWorkunitSourceFiles } from "../hooks/workunit";
import { pushParams } from "../util/history";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
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
const defaultSort = { attribute: undefined, descending: false };

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
                label: "Name", width: 400, sortable: true,
                formatter: (Name, row) => {
                    let fileUrl = `#/files/${Name}`;
                    if (row?.FileCluster) {
                        fileUrl = `#/files/${row.FileCluster}/${Name}`;
                    }
                    return <>
                        <Image src={Utility.getImageURL(row.IsSuperFile ? "folder_table.png" : "file.png")} />
                        &nbsp;
                        <Link href={fileUrl}>{Name}</Link>
                    </>;
                }
            },
            FileCluster: { label: nlsHPCC.FileCluster, width: 200, sortable: false },
            Count: { label: nlsHPCC.Usage, width: 72, sortable: true, justify: "right" }
        };
    }, []);

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
                    let fileUrl = `#/files/${selection[0].Name}`;
                    if (selection[0]?.FileCluster) {
                        fileUrl = `#/files/${selection[0].FileCluster}/${selection[0].Name}`;
                    }
                    window.location.href = fileUrl;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        let fileUrl = `#/files/${selection[i].Name}`;
                        if (selection[i]?.FileCluster) {
                            fileUrl = `#/files/${selection[i].FileCluster}/${selection[i].Name}`;
                        }
                        window.open(fileUrl, "_blank");
                    }
                }
            }
        },
        {
            key: "filter", text: nlsHPCC.Filter, disabled: data?.length == 0, iconProps: { iconName: hasFilter ? "FilterSolid" : "Filter" },
            onClick: () => { setShowFilter(true); }
        },
    ], [data.length, hasFilter, refreshData, selection, uiState.hasSelection]);

    const copyButtons = useCopyButtons(columns, selection, "sourceFiles");

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

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <div style={{ position: "relative", height: "100%" }}>
                <FluentGrid
                    data={data}
                    primaryID={"Name"}
                    alphaNumColumns={{ Value: true }}
                    sort={sort}
                    columns={columns}
                    setSelection={setSelection}
                    setTotal={setTotal}
                    refresh={refreshTable}
                ></FluentGrid>
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
            </div>
        }
    />;
};
