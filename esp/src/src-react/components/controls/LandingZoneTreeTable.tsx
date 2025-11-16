import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { Checkbox, Table, TableHeader, TableRow, TableHeaderCell, TableBody, TableCell, Button, Spinner, makeStyles, mergeClasses, tokens } from "@fluentui/react-components";
import { useConst } from "@fluentui/react-hooks";
import { FolderRegular, DocumentRegular, ServerRegular, DesktopRegular, ChevronDownRegular, ChevronRightRegular } from "@fluentui/react-icons";
import { userKeyValStore } from "src/KeyValStore";
import { convertedSize } from "src/Utility";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/controls/LandingZoneTreeTable.tsx");

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
    table: {
        width: "100%",
        tableLayout: "fixed"
    },
    resizableHeaderCell: {
        position: "relative",
        overflow: "hidden",
        minWidth: "60px",
        userSelect: "none"
    },
    resizeHandle: {
        position: "absolute",
        right: "-8px",
        top: 0,
        bottom: 0,
        width: "4px",
        cursor: "ew-resize",
        backgroundColor: "transparent",
        ":hover": {
            backgroundColor: tokens.colorNeutralBackground5
        }
    },
    nameCell: {
        display: "flex",
        alignItems: "center",
        gap: "8px",
        overflow: "hidden"
    },
    indentedRow: {
        paddingLeft: "24px"
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

const OPEN_ITEMS_STORAGE_KEY = "LandingZone_ExpandedItems";

const usePersistedExpansion = () => {
    const store = useConst(() => userKeyValStore());
    const [expandedItems, setExpandedItems] = React.useState<Set<string>>(new Set());
    const [isLoaded, setIsLoaded] = React.useState(false);

    React.useEffect(() => {
        store.get(OPEN_ITEMS_STORAGE_KEY).then((stored) => {
            if (stored) {
                try {
                    const parsed = JSON.parse(stored);
                    if (Array.isArray(parsed)) {
                        setExpandedItems(new Set(parsed));
                    }
                } catch (err) {
                    logger.error(nlsHPCC.StorageStateParseFailure);
                }
            }
            setIsLoaded(true);
        }).catch(() => {
            setIsLoaded(true);
        });
    }, [store]);

    const updateExpandedItems = React.useCallback((newExpandedItems: Set<string>) => {
        setExpandedItems(newExpandedItems);
        store.set(OPEN_ITEMS_STORAGE_KEY, JSON.stringify(Array.from(newExpandedItems))).catch(_err => {
            logger.error(nlsHPCC.StorageStateWriteFailure);
        });
    }, [store]);

    return { expandedItems, updateExpandedItems, isLoaded };
};

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
    const { expandedItems: persistedExpandedItems, updateExpandedItems, isLoaded } = usePersistedExpansion();
    const hasNotifiedParent = React.useRef(false);

    const mergedExpandedItems = React.useMemo(() => {
        if (!isLoaded) {
            return externalExpandedItems;
        }
        return new Set([...externalExpandedItems, ...persistedExpandedItems]);
    }, [externalExpandedItems, persistedExpandedItems, isLoaded]);

    React.useEffect(() => {
        if (isLoaded && !hasNotifiedParent.current && persistedExpandedItems.size > 0) {
            hasNotifiedParent.current = true;
            onExpansionChange(new Set(persistedExpandedItems));
        }
    }, [isLoaded, persistedExpandedItems, onExpansionChange]);


    const [columnWidths, setColumnWidths] = React.useState({
        selection: 60,
        name: 300,
        size: 120,
        modified: 180
    });

    const [isResizing, setIsResizing] = React.useState<string | null>(null);
    const [startX, setStartX] = React.useState(0);
    const [startWidth, setStartWidth] = React.useState(0);

    const handleResizeMouseDown = React.useCallback((columnId: keyof typeof columnWidths) => (evt: React.MouseEvent) => {
        setIsResizing(columnId);
        setStartX(evt.clientX);
        setStartWidth(columnWidths[columnId]);
        evt.preventDefault();
    }, [columnWidths]);

    const handleResizeMouseMove = React.useCallback((evt: MouseEvent) => {
        if (!isResizing) return;

        const diff = evt.clientX - startX;
        const newWidth = Math.max(60, startWidth + diff); // minimum width of 60px

        setColumnWidths(prev => ({
            ...prev,
            [isResizing]: newWidth
        }));
    }, [isResizing, startX, startWidth]);

    const handleResizeMouseUp = React.useCallback(() => {
        setIsResizing(null);
    }, []);

    React.useEffect(() => {
        if (isResizing) {
            document.addEventListener("mousemove", handleResizeMouseMove);
            document.addEventListener("mouseup", handleResizeMouseUp);
            return () => {
                document.removeEventListener("mousemove", handleResizeMouseMove);
                document.removeEventListener("mouseup", handleResizeMouseUp);
            };
        }
    }, [isResizing, handleResizeMouseMove, handleResizeMouseUp]);

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

    const getIndentLevel = React.useCallback((level: number) => ({
        paddingLeft: `${level * 20}px`
    }), []);

    const handleExpansionToggle = React.useCallback((item: LandingZoneItem) => {
        const newExpandedItems = new Set(mergedExpandedItems);
        if (mergedExpandedItems.has(item.id)) {
            newExpandedItems.delete(item.id);
        } else {
            newExpandedItems.add(item.id);
        }

        updateExpandedItems(newExpandedItems);
        onExpansionChange(newExpandedItems);
    }, [mergedExpandedItems, updateExpandedItems, onExpansionChange]);

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

    if (loading) {
        return <div className={styles.loadingContainer}>
            <Spinner label={nlsHPCC.Loading} />
        </div>;
    }

    return <div className={styles.container}>
        <div className={styles.tableContainer}>
            <Table className={styles.table} size="small">
                <TableHeader className={styles.tableHeader}>
                    <TableRow>
                        <TableHeaderCell className={mergeClasses(styles.resizableHeaderCell, styles.tableCell)} style={{ width: `${columnWidths.selection}px` }} >
                            <Checkbox
                                className={styles.roundCheckbox}
                                checked={items.some(item => item.type === "file") && items.filter(item => item.type === "file").every(item => selectedItems.has(item.id))}
                                onChange={handleSelectAll}
                            />
                            <div className={styles.resizeHandle} onMouseDown={handleResizeMouseDown("selection")} />
                        </TableHeaderCell>
                        <TableHeaderCell className={mergeClasses(styles.resizableHeaderCell, styles.tableCell)} style={{ width: `${columnWidths.name}px` }}>
                            {nlsHPCC.Name}
                            <div className={styles.resizeHandle} onMouseDown={handleResizeMouseDown("name")} />
                        </TableHeaderCell>
                        <TableHeaderCell className={mergeClasses(styles.resizableHeaderCell, styles.tableCell)} style={{ width: `${columnWidths.size}px` }}>
                            {nlsHPCC.Size}
                            <div className={styles.resizeHandle} onMouseDown={handleResizeMouseDown("size")} />
                        </TableHeaderCell>
                        <TableHeaderCell className={mergeClasses(styles.resizableHeaderCell, styles.tableCell)} style={{ width: `${columnWidths.modified}px` }}>
                            {nlsHPCC.Date}
                            <div className={styles.resizeHandle} onMouseDown={handleResizeMouseDown("modified")} />
                        </TableHeaderCell>
                    </TableRow>
                </TableHeader>
                <TableBody>
                    {items.map((item) => (
                        <TableRow
                            key={item.id}
                            className={item.type === "file" ? styles.fileRow : undefined}
                            onClick={(evt) => handleRowClick(item, evt)}
                        >
                            <TableCell className={styles.tableCell} style={{ width: `${columnWidths.selection}px` }}>
                                <div style={getIndentLevel(item.level)}>
                                    {(item.type === "file") ? (
                                        <Checkbox className={styles.roundCheckbox} checked={selectedItems.has(item.id)} onChange={() => handleSelectionToggle(item)} />
                                    ) : (
                                        <div style={{ width: "20px" }} />
                                    )}
                                </div>
                            </TableCell>
                            <TableCell className={styles.tableCell} style={{ width: `${columnWidths.name}px` }}>
                                <div className={styles.nameCell} style={getIndentLevel(item.level)}>
                                    {item.hasChildren ? (
                                        <Button
                                            appearance="subtle"
                                            size="small"
                                            icon={
                                                loadingItems.has(item.id) ? (
                                                    <Spinner size="extra-small" className={styles.compactSpinner} />
                                                ) : mergedExpandedItems.has(item.id) ? (
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
                            </TableCell>
                            <TableCell className={styles.tableCell} style={{ width: `${columnWidths.size}px` }}>
                                <span>{item.size ? convertedSize(item.size) : ""}</span>
                            </TableCell>
                            <TableCell className={styles.tableCell} style={{ width: `${columnWidths.modified}px` }}>
                                <span>{item.modifiedTime || ""}</span>
                            </TableCell>
                        </TableRow>
                    ))}
                </TableBody>
            </Table>
        </div>
    </div>;
};
