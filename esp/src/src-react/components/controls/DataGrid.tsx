import * as React from "react";
import { DataGridBody, DataGridProps, DataGridRow, DataGrid, DataGridHeader, DataGridHeaderCell, DataGridCell } from "@fluentui/react-components";

export type SortState = Parameters<NonNullable<DataGridProps["onSortChange"]>>[1];

interface DataGridV9Props {
    items: any,
    columns: any,
    onSelect?: any,
    onSortChange?: any,
    selectedItems?: any,
    sizingOptions?: any,
    sortState?: any,
}

export const DataGridV9: React.FunctionComponent<DataGridV9Props> = ({
    items,
    columns,
    selectedItems,
    sortState,
    onSelect,
    onSortChange,
    sizingOptions
}) => {
    return <DataGrid
        items={items}
        columns={columns}
        resizableColumns
        columnSizingOptions={sizingOptions}
        sortable
        sortState={sortState}
        onSortChange={onSortChange}
        style={{ overflow: "hidden" }}
        selectionMode="multiselect"
        selectedItems={selectedItems ?? new Set()}
        onSelectionChange={(e, data) => {
            console.log(data);
            if (onSelect) onSelect(items.filter((el, idx) => data.selectedItems.has(idx)), data.selectedItems);
        }}
        focusMode="composite"
    >
        <DataGridHeader>
            <DataGridRow selectionCell={{ checkboxIndicator: { "aria-label": "Select all rows" }, }}>
                {({ renderHeaderCell }) => (
                    <DataGridHeaderCell>{renderHeaderCell()}</DataGridHeaderCell>
                )}
            </DataGridRow>
        </DataGridHeader>
        <DataGridBody<typeof items[0]>>
            {({ item, rowId }) => (
                <DataGridRow<typeof item> key={rowId} selectionCell={{ checkboxIndicator: { "aria-label": "Select row" }, }}>
                    {({ renderCell }) => (
                        <DataGridCell>{renderCell(item)}</DataGridCell>
                    )}
                </DataGridRow>
            )}
        </DataGridBody>
    </DataGrid>;
};