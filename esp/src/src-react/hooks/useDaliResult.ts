import * as React from "react";
import { csvParse } from "d3-dsv";

export interface DaliColumn {
    key: string;
    name: string;
    fieldName: string;
    minWidth?: number;
}

export interface DaliResultState {
    columns: DaliColumn[];
    items: object[];
    setResult: (csv: string) => void;
    setColumns: React.Dispatch<React.SetStateAction<DaliColumn[]>>;
    setItems: React.Dispatch<React.SetStateAction<object[]>>;
}

/**
 * Shared state hook for Dali admin components.
 *
 * Owns the columns/items state for the result Table and provides:
 *  - `setResult(csv)` – parses a CSV string and sets both columns and items
 *  - `setColumns` / `setItems` – raw dispatchers for non-CSV responses (e.g. DaliCount)
 */
export function useDaliResult(): DaliResultState {
    const [columns, setColumns] = React.useState<DaliColumn[]>([]);
    const [items, setItems] = React.useState<object[]>([]);

    const setResult = React.useCallback((csv: string) => {
        const data = csvParse(csv);
        setColumns(data.columns.map(col => ({
            key: col,
            name: col,
            fieldName: col,
            minWidth: 100,
        })));
        setItems(data);
    }, []);

    return { columns, items, setResult, setColumns, setItems };
}
