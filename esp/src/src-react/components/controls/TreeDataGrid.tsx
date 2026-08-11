import * as React from "react";
import { DataGrid, DataGridBody, DataGridCell, DataGridHeader, DataGridHeaderCell, DataGridRow, TableColumnDefinition, TableColumnSizingOptions, makeStyles, tokens } from "@fluentui/react-components";
import { SizeMe } from "../../layouts/SizeMe";

const useStyles = makeStyles({
    container: {
        display: "flex",
        flexDirection: "column",
        height: "100%"
    },
    tableContainer: {
        flex: 1,
        overflowX: "hidden",
        overflowY: "auto"
    },
    grid: {
        width: "100%"
    },
    selectableRow: {
        userSelect: "text",
        ":hover": {
            backgroundColor: tokens.colorNeutralBackground1Hover
        }
    },
    tableCell: {
        borderBottom: `1px solid ${tokens.colorNeutralBackground5}`,
        borderRight: `1px solid ${tokens.colorNeutralBackground5}`,
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap"
    },
    tableHeader: {
        "& tr": {
            borderBottom: `1px solid ${tokens.colorNeutralBackground5} !important`
        }
    },
});

interface TreeDataGridProps<T extends { hpcc_id: string }> {
    items: T[];
    columns: TableColumnDefinition<T>[];
    columnSizingOptions?: TableColumnSizingOptions;
    isSelectable?: (item: T) => boolean;
    onRowClick?: (item: T, evt: React.MouseEvent) => void;
    onRowDoubleClick?: (item: T, evt: React.MouseEvent) => void;
}

export function TreeDataGrid<T extends { hpcc_id: string }>({
    items,
    columns,
    columnSizingOptions,
    isSelectable,
    onRowClick,
    onRowDoubleClick
}: TreeDataGridProps<T>): React.ReactElement {
    const styles = useStyles();

    return <SizeMe>{({ size }) => (
        <div style={{ position: "relative", width: "100%", height: "100%" }}>
            <div style={{ position: "absolute", width: "100%", height: `${size.height}px`, overflowY: "auto" }}>
                <div className={styles.container}>
                    <div className={styles.tableContainer}>
                        <DataGrid
                            className={styles.grid}
                            items={items}
                            columns={columns}
                            getRowId={(item) => item.hpcc_id}
                            resizableColumns
                            columnSizingOptions={columnSizingOptions}
                            focusMode="cell"
                            size="small"
                        >
                            <DataGridHeader className={styles.tableHeader}>
                                <DataGridRow<T>>
                                    {({ renderHeaderCell }) => (
                                        <DataGridHeaderCell className={styles.tableCell}>
                                            {renderHeaderCell()}
                                        </DataGridHeaderCell>
                                    )}
                                </DataGridRow>
                            </DataGridHeader>
                            <DataGridBody<T>>
                                {({ item, rowId }) => {
                                    const selectable = isSelectable?.(item) ?? false;
                                    return (
                                        <DataGridRow<T>
                                            key={rowId}
                                            className={selectable ? styles.selectableRow : undefined}
                                            onClick={onRowClick ? (evt) => onRowClick(item, evt) : undefined}
                                            onDoubleClick={onRowDoubleClick ? (evt) => onRowDoubleClick(item, evt) : undefined}
                                        >
                                            {({ renderCell }) => (
                                                <DataGridCell className={styles.tableCell}>
                                                    {renderCell(item)}
                                                </DataGridCell>
                                            )}
                                        </DataGridRow>
                                    );
                                }}
                            </DataGridBody>
                        </DataGrid>
                    </div>
                </div>
            </div>
        </div>
    )}</SizeMe>;
}
