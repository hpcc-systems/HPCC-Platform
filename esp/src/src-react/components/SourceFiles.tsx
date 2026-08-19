import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "./CommandBarV9";
import { Button, Checkbox, createTableColumn, Link, TableColumnDefinition, TableColumnSizingOptions } from "@fluentui/react-components";
import { ChevronDownRegular, ChevronRightRegular } from "@fluentui/react-icons";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useWorkunitSourceFiles } from "../hooks/workunit";
import { useTreeExpansion } from "../hooks/useTreeExpansion";
import { useTreeTableSelection } from "../hooks/useTreeTableSelection";
import { pushParams } from "../util/history";
import { HolyGrail } from "../layouts/HolyGrail";
import { useCopyButtons, FluentColumns } from "./controls/Grid";
import { TreeDataGrid } from "./controls/TreeDataGrid";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";

const FilterFields: Fields = {
    "Name": { type: "string", label: nlsHPCC.Name, placeholder: nlsHPCC.TargetNamePlaceholder },
};

const copyColumns: FluentColumns = {
    selection: { label: "", field: "selection", selectorType: "checkbox" },
    name: { label: nlsHPCC.Name, field: "Name" },
    fileCluster: { label: nlsHPCC.FileCluster, field: "FileCluster" },
    count: { label: nlsHPCC.Usage, field: "Count" },
};

interface SourceFileRow {
    hpcc_id: string;
    Name?: string;
    FileCluster?: string;
    IsSuperFile?: boolean;
    Count?: number;
    __hpcc_parentName: string;
    children?: SourceFileRow[];
}

interface SourceFilesProps {
    wuid: string;
    filter?: { [id: string]: any };
}

const emptyFilter: { [id: string]: any } = {};

export const SourceFiles: React.FunctionComponent<SourceFilesProps> = ({
    wuid,
    filter = emptyFilter,
}) => {

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);

    const [sourceFiles, , , refreshData] = useWorkunitSourceFiles(wuid);
    const [showFilter, setShowFilter] = React.useState(false);

    const { expandedItems, toggle } = useTreeExpansion();

    const treeData = React.useMemo((): SourceFileRow[] => {
        const nameFilter = filter?.Name as string | undefined;
        const makeRowId = (f: { __hpcc_parentName: string; FileCluster?: string; Name?: string }) => `${f.__hpcc_parentName}_${f.FileCluster ?? ""}_${f.Name ?? ""}`;
        const nameRE = nameFilter ? new RegExp(
            nameFilter
                .replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
                .replace(/\\\*/g, ".*"),
            "i"
        ) : undefined;
        const matches = (f: { Name?: string }) => !nameRE || !!f.Name?.match(nameRE);
        const childMap: Record<string, SourceFileRow[]> = {};
        sourceFiles
            .filter(f => f.__hpcc_parentName !== "")
            .forEach(f => {
                const key = f.__hpcc_parentName;
                if (!childMap[key]) childMap[key] = [];
                childMap[key].push({ ...f, hpcc_id: makeRowId(f) });
            });
        return sourceFiles
            .filter(f => f.__hpcc_parentName === "")
            .map(f => {
                const children = (childMap[f.Name ?? ""] ?? []).filter(matches);
                if (!matches(f) && children.length === 0) return undefined;
                return {
                    ...f,
                    hpcc_id: makeRowId(f),
                    children
                };
            })
            .filter((f) => !!f) as SourceFileRow[];
    }, [filter, sourceFiles]);

    const isSelectable = React.useCallback(() => true, []);

    const { selectedItems, getVisibleItems, handleSelectionToggle, handleSelectAll, handleRowClick } = useTreeTableSelection({
        isSelectable,
        items: treeData,
        expandedItems
    });

    const visibleItems = React.useMemo(() => getVisibleItems(), [getVisibleItems]);

    const allSelected = visibleItems.length > 0 && visibleItems.every(item => selectedItems.has(item.hpcc_id));
    const someSelected = !allSelected && visibleItems.some(item => selectedItems.has(item.hpcc_id));

    const selectedFileItems = React.useMemo((): SourceFileRow[] => {
        const result: SourceFileRow[] = [];
        const addFromTree = (items: SourceFileRow[]) => {
            items.forEach(item => {
                if (selectedItems.has(item.hpcc_id)) result.push(item);
                if (item.children) addFromTree(item.children);
            });
        };
        addFromTree(treeData);
        return result;
    }, [treeData, selectedItems]);

    const columnSizingOptions = React.useMemo<TableColumnSizingOptions>(() => ({
        selection: { minWidth: 24, idealWidth: 24 },
        name: { minWidth: 180, idealWidth: 400 },
        fileCluster: { minWidth: 120, idealWidth: 200 },
        count: { minWidth: 60, idealWidth: 72 },
    }), []);

    //  Grid ---
    const columns = React.useMemo((): TableColumnDefinition<SourceFileRow>[] => [
        createTableColumn<SourceFileRow>({
            columnId: "selection",
            renderHeaderCell: () => (
                <Checkbox
                    checked={allSelected ? true : someSelected ? "mixed" : false}
                    onChange={handleSelectAll}
                />
            ),
            renderCell: (item) => (
                <Checkbox
                    checked={selectedItems.has(item.hpcc_id)}
                    onChange={() => handleSelectionToggle(item)}
                />
            )
        }),
        createTableColumn<SourceFileRow>({
            columnId: "name",
            renderHeaderCell: () => nlsHPCC.Name,
            renderCell: (item) => {
                const fileUrl = item.FileCluster ? `#/files/${item.FileCluster}/${item.Name}` : `#/files/${item.Name}`;
                return (
                    <div style={{ display: "flex", alignItems: "center", gap: "8px", overflow: "hidden", paddingLeft: item.__hpcc_parentName ? "20px" : "0" }}>
                        {item.__hpcc_parentName === "" && (item.children?.length ?? 0) > 0 ? (
                            <Button
                                appearance="subtle"
                                size="small"
                                icon={expandedItems.has(item.hpcc_id) ? <ChevronDownRegular /> : <ChevronRightRegular />}
                                onClick={(evt) => { evt.stopPropagation(); toggle(item.hpcc_id); }}
                            />
                        ) : (
                            <div style={{ width: "24px", flexShrink: 0 }} />
                        )}
                        <img src={Utility.getImageURL(item.IsSuperFile ? "folder_table.png" : "file.png")} alt="" style={{ flexShrink: 0 }} />
                        <Link href={fileUrl} style={{ overflow: "hidden", textOverflow: "ellipsis" }}>{item.Name}</Link>
                    </div>
                );
            }
        }),
        createTableColumn<SourceFileRow>({
            columnId: "fileCluster",
            renderHeaderCell: () => nlsHPCC.FileCluster,
            renderCell: (item) => <span>{item.FileCluster ?? ""}</span>
        }),
        createTableColumn<SourceFileRow>({
            columnId: "count",
            renderHeaderCell: () => nlsHPCC.Usage,
            renderCell: (item) => <span>{item.Count !== undefined ? item.Count : ""}</span>
        }),
    ], [allSelected, expandedItems, handleSelectAll, handleSelectionToggle, selectedItems, someSelected, toggle]);

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
        { key: "divider_1", itemType: ContextualMenuItemType.Divider },
        {
            key: "open", text: nlsHPCC.Open, disabled: selectedFileItems.length === 0, iconProps: { iconName: "WindowEdit" },
            onClick: () => {
                if (selectedFileItems.length === 1) {
                    const f = selectedFileItems[0];
                    window.location.href = f.FileCluster ? `#/files/${f.FileCluster}/${f.Name}` : `#/files/${f.Name}`;
                } else {
                    for (let i = selectedFileItems.length - 1; i >= 0; --i) {
                        const f = selectedFileItems[i];
                        window.open(f.FileCluster ? `#/files/${f.FileCluster}/${f.Name}` : `#/files/${f.Name}`, "_blank");
                    }
                }
            }
        },
        {
            key: "filter", text: nlsHPCC.Filter, iconProps: { iconName: hasFilter ? "FilterSolid" : "Filter" },
            onClick: () => { setShowFilter(true); }
        },
    ], [hasFilter, refreshData, selectedFileItems]);

    const copyButtons = useCopyButtons(copyColumns, selectedFileItems, "sourceFiles");

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <div style={{ position: "relative", height: "100%" }}>
                <TreeDataGrid
                    items={visibleItems}
                    columns={columns}
                    columnSizingOptions={columnSizingOptions}
                    isSelectable={isSelectable}
                    onRowClick={handleRowClick}
                />
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
            </div>
        }
    />;
};
