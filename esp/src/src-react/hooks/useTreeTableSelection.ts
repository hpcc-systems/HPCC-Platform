import * as React from "react";

interface UseTreeTableSelectionOptions<T extends { hpcc_id: string; children?: T[] }> {
    selectedItems?: Set<string>;
    onSelectionChange?: (selectedIds: Set<string>) => void;
    onRowDoubleClick?: (item: T) => void;
    isSelectable: (item: T) => boolean;
    items: T[];
    expandedItems: Set<string>;
}

export const useTreeTableSelection = <T extends { hpcc_id: string; children?: T[] }>({
    selectedItems: selectedItemsProp,
    onSelectionChange,
    onRowDoubleClick,
    isSelectable,
    items,
    expandedItems
}: UseTreeTableSelectionOptions<T>) => {
    const [internalSelectedItems, setInternalSelectedItems] = React.useState<Set<string>>(new Set());

    const selectedItems = selectedItemsProp ?? internalSelectedItems;

    const setSelectedItems = React.useCallback((newItems: Set<string>) => {
        if (onSelectionChange) {
            onSelectionChange(newItems);
        } else {
            setInternalSelectedItems(newItems);
        }
    }, [onSelectionChange]);

    const getVisibleItems = React.useCallback((): T[] => {
        const visible: T[] = [];

        const addItems = (nodes: T[]) => {
            nodes.forEach(item => {
                visible.push(item);
                if (expandedItems.has(item.hpcc_id) && item.children && item.children.length > 0) {
                    addItems(item.children);
                }
            });
        };

        addItems(items);
        return visible;
    }, [items, expandedItems]);

    const handleSelectionToggle = React.useCallback((item: T) => {
        if (!isSelectable(item)) return;

        const newSelectedItems = new Set(selectedItems);
        if (selectedItems.has(item.hpcc_id)) {
            newSelectedItems.delete(item.hpcc_id);
        } else {
            newSelectedItems.add(item.hpcc_id);
        }
        setSelectedItems(newSelectedItems);
    }, [isSelectable, selectedItems, setSelectedItems]);

    const handleSelectAll = React.useCallback(() => {
        const selectableItems = getVisibleItems().filter(isSelectable);

        if (selectableItems.length === 0 || selectableItems.every(item => selectedItems.has(item.hpcc_id))) {
            setSelectedItems(new Set());
        } else {
            const newSelection = new Set(selectedItems);
            selectableItems.forEach(item => newSelection.add(item.hpcc_id));
            setSelectedItems(newSelection);
        }
    }, [getVisibleItems, isSelectable, selectedItems, setSelectedItems]);

    const handleRowClick = React.useCallback((item: T, evt: React.MouseEvent) => {
        if (!isSelectable(item)) return;

        const selection = window.getSelection();
        if (selection && selection.toString().length > 0) return;

        const target = evt.target as HTMLElement;
        if (target.closest("button, a, input[type=\"checkbox\"], .fui-Checkbox")) return;

        handleSelectionToggle(item);
    }, [isSelectable, handleSelectionToggle]);

    const handleDoubleClick = React.useCallback((item: T, evt: React.MouseEvent) => {
        evt.preventDefault();
        if (onRowDoubleClick) {
            onRowDoubleClick(item);
        }
    }, [onRowDoubleClick]);

    const clearSelection = React.useCallback(() => {
        setSelectedItems(new Set());
    }, [setSelectedItems]);

    return {
        selectedItems,
        clearSelection,
        getVisibleItems,
        handleSelectionToggle,
        handleSelectAll,
        handleRowClick,
        handleDoubleClick
    };
};
