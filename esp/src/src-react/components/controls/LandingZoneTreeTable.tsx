import * as React from "react";
import { Button, Checkbox, createTableColumn, DataGrid, DataGridBody, DataGridCell, DataGridHeader, DataGridHeaderCell, DataGridRow, Spinner, TableColumnDefinition, TableColumnSizingOptions, makeStyles, tokens } from "@fluentui/react-components";
import { FolderRegular, DocumentRegular, ServerRegular, DesktopRegular, ChevronDownRegular, ChevronRightRegular } from "@fluentui/react-icons";
import { convertedSize } from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useTreeExpansion } from "../../hooks/useTreeExpansion";

const useStyles = makeStyles({
    container: {
        display: "flex",
        flexDirection: "column",
        height: "100%"
    },
    tableContainer: {
        flex: 1,
        overflow: "auto"
    },
    grid: {
        width: "100%"
    },
    nameCell: {
        display: "flex",
        alignItems: "center",
        gap: "8px",
        overflow: "hidden"
    },
    loadingContainer: {
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
        height: "200px"
    },
    compactSpinner: {
        "& > span": {
            width: "12px !important",
            height: "12px !important"
        }
    },
    roundCheckbox: {
        "& .fui-Checkbox__indicator": {
            borderRadius: "50%",
            width: "18px",
            height: "18px"
        },
        "& .fui-Checkbox__input:checked + .fui-Checkbox__indicator": {
            borderRadius: "50%",
            border: "1px solid white",
            width: "18px",
            height: "18px"
        }
    },
    selectableRow: {
        userSelect: "text",
        ":hover": {
            backgroundColor: tokens.colorNeutralBackground1Hover
        }
    },
    fileRow: {
        userSelect: "text",
        ":hover": {
            backgroundColor: tokens.colorNeutralBackground1Hover
        }
    },
    tableCell: {
        borderBottom: `1px solid ${tokens.colorNeutralBackground5}`,
        borderRight: `1px solid ${tokens.colorNeutralBackground5}`
    },
    tableHeader: {
        "& tr": {
            borderBottom: `1px solid ${tokens.colorNeutralBackground5} !important`
        }
    }
});

const getIcon = (item: LandingZoneItem) => {
    switch (item.type) {
        case "dropzone":
            return <ServerRegular />;
        case "machine":
            return <DesktopRegular />;
        case "folder":
            return <FolderRegular />;
        case "file":
            return <DocumentRegular />;
        default:
            return <DocumentRegular />;
    }
};

const EXPANSION_STORAGE_KEY = "LandingZone_ExpandedItems";

export interface LandingZoneItem {
    id: string;
    name: string;
    displayName: string;
    type: "dropzone" | "machine" | "folder" | "file";
    parentId?: string;
    size?: number;
    modifiedTime?: string;
    isDirectory?: boolean;
    level: number;
    hasChildren: boolean;
    isExpanded?: boolean;
    isSelected?: boolean;
    data: any;
}

interface LandingZoneTreeTableProps {
    items: LandingZoneItem[];
    loading?: boolean;
    loadingItems?: Set<string>;
    selectedItems: Set<string>;
    expandedItems: Set<string>;
    onSelectionChange: (selectedIds: Set<string>) => void;
    onExpansionChange: (expandedIds: Set<string>) => void;
}

export const LandingZoneTreeTable: React.FunctionComponent<LandingZoneTreeTableProps> = ({
    items,
    loading = false,
    loadingItems = new Set(),
    selectedItems,
    expandedItems: externalExpandedItems,
    onSelectionChange,
    onExpansionChange
}) => {
    const styles = useStyles();
    const { expandedItems, isLoaded, toggle } = useTreeExpansion(EXPANSION_STORAGE_KEY, externalExpandedItems);
    const hasNotifiedParent = React.useRef(false);

    // Notify parent when persisted expansion items are loaded
    React.useEffect(() => {
        if (isLoaded && !hasNotifiedParent.current && expandedItems.size > externalExpandedItems.size) {
            hasNotifiedParent.current = true;
            onExpansionChange(new Set(expandedItems));
        }
    }, [isLoaded, expandedItems, externalExpandedItems, onExpansionChange]);

    const visibleItems = items;
    const selectableItems = visibleItems.filter(item => item.type === "file");
    const allSelectableSelected = selectableItems.length > 0 && selectableItems.every(item => selectedItems.has(item.id));

    const getIndentLevel = React.useCallback((level: number) => ({
        paddingLeft: `${level * 20}px`
    }), []);

    const handleExpansionToggle = React.useCallback((item: LandingZoneItem) => {
        const newExpandedItems = toggle(item.id);
        onExpansionChange(new Set(newExpandedItems));
    }, [toggle, onExpansionChange]);

    const handleSelectionToggle = React.useCallback((item: LandingZoneItem) => {
        if (item.type === "dropzone" || item.type === "machine" || item.type === "folder") {
            // don't allow selection of anything not files
            return;
        }

        const newSelectedItems = new Set(selectedItems);
        if (selectedItems.has(item.id)) {
            newSelectedItems.delete(item.id);
        } else {
            newSelectedItems.add(item.id);
        }
        onSelectionChange(newSelectedItems);
    }, [selectedItems, onSelectionChange]);

    const handleSelectAll = React.useCallback(() => {
        const selectableItems = items.filter(item =>
            item.type === "file" && !selectedItems.has(item.id)
        );

        if (selectableItems.length === 0) {
            onSelectionChange(new Set());
        } else {
            const newSelection = new Set(selectedItems);
            selectableItems.forEach(item => newSelection.add(item.id));
            onSelectionChange(newSelection);
        }
    }, [items, selectedItems, onSelectionChange]);

    const handleRowClick = React.useCallback((item: LandingZoneItem, evt: React.MouseEvent) => {
        // only handle selection of files
        if (item.type !== "file") return;

        // allow selection of text in the rows
        const selection = window.getSelection();
        if (selection && selection.toString().length > 0) return;

        // don't trigger if clicking on elements like buttons or checkboxes
        const target = evt.target as HTMLElement;
        if (target.closest("button, input[type=\"checkbox\"], .fui-Checkbox")) return;

        handleSelectionToggle(item);
    }, [handleSelectionToggle]);

    const columnSizingOptions = React.useMemo<TableColumnSizingOptions>(() => ({
        selection: { minWidth: 24, idealWidth: 24 },
        name: { minWidth: 220, idealWidth: 420 },
        size: { minWidth: 100, idealWidth: 140 },
        modified: { minWidth: 120, idealWidth: 180 }
    }), []);

    const columns = React.useMemo<TableColumnDefinition<LandingZoneItem>[]>(() => [
        createTableColumn<LandingZoneItem>({
            columnId: "selection",
            renderHeaderCell: () => (
                <Checkbox
                    className={styles.roundCheckbox}
                    checked={allSelectableSelected}
                    onChange={handleSelectAll}
                />
            ),
            renderCell: (item) => item.type === "file" ? (
                <Checkbox className={styles.roundCheckbox} checked={selectedItems.has(item.id)} onChange={() => handleSelectionToggle(item)} />
            ) : (
                <div style={{ width: "20px" }} />
            )
        }),
        createTableColumn<LandingZoneItem>({
            columnId: "name",
            renderHeaderCell: () => nlsHPCC.Name,
            renderCell: (item) => (
                <div className={styles.nameCell} style={getIndentLevel(item.level)}>
                    {item.hasChildren ? (
                        <Button
                            appearance="subtle"
                            size="small"
                            icon={
                                loadingItems.has(item.id) ? (
                                    <Spinner size="extra-small" className={styles.compactSpinner} />
                                ) : expandedItems.has(item.id) ? (
                                    <ChevronDownRegular />
                                ) : (
                                    <ChevronRightRegular />
                                )
                            }
                            onClick={() => handleExpansionToggle(item)}
                            disabled={loadingItems.has(item.id)}
                        />
                    ) : (
                        <div style={{ width: "24px" }} />
                    )}
                    {getIcon(item)}
                    <span>{item.displayName}</span>
                </div>
            )
        }),
        createTableColumn<LandingZoneItem>({
            columnId: "size",
            renderHeaderCell: () => nlsHPCC.FileSize,
            renderCell: (item) => <span>{item.size ? convertedSize(item.size) : ""}</span>
        }),
        createTableColumn<LandingZoneItem>({
            columnId: "modified",
            renderHeaderCell: () => nlsHPCC.Date,
            renderCell: (item) => <span>{item.modifiedTime || ""}</span>
        })
    ], [allSelectableSelected, expandedItems, getIndentLevel, handleExpansionToggle, handleSelectAll, handleSelectionToggle, loadingItems, selectedItems, styles.compactSpinner, styles.nameCell, styles.roundCheckbox]);

    if (loading) {
        return <div className={styles.loadingContainer}>
            <Spinner label={nlsHPCC.Loading} />
        </div>;
    }

    return <div className={styles.container}>
        <div className={styles.tableContainer}>
            <DataGrid
                className={styles.grid}
                items={visibleItems}
                columns={columns}
                getRowId={(item) => item.id}
                resizableColumns
                columnSizingOptions={columnSizingOptions}
                focusMode="cell"
                size="small"
            >
                <DataGridHeader className={styles.tableHeader}>
                    <DataGridRow<LandingZoneItem>>
                        {({ renderHeaderCell }) => (
                            <DataGridHeaderCell className={styles.tableCell}>
                                {renderHeaderCell()}
                            </DataGridHeaderCell>
                        )}
                    </DataGridRow>
                </DataGridHeader>
                <DataGridBody<LandingZoneItem>>
                    {({ item, rowId }) => (
                        <DataGridRow<LandingZoneItem>
                            key={rowId}
                            className={item.type === "file" ? styles.fileRow : undefined}
                            onClick={(evt) => handleRowClick(item, evt)}
                        >
                            {({ renderCell }) => (
                                <DataGridCell className={styles.tableCell}>
                                    {renderCell(item)}
                                </DataGridCell>
                            )}
                        </DataGridRow>
                    )}
                </DataGridBody>
            </DataGrid>
        </div>
    </div>;
};
