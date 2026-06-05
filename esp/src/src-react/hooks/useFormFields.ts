import * as React from "react";

/**
 * Manages a record of form field values as a single state object,
 * replacing per-field useState + switch-based onChange boilerplate.
 *
 * @example
 * const [fields, handleChange] = useFormFields({ FileName: "", PartNumber: 1 });
 * // fields.FileName, fields.PartNumber are fully typed
 * // handleChange can be passed directly as TableGroup onChange
 */
export function useFormFields<T extends Record<string, any>>(initial: T): [T, (id: string, value: any) => void] {
    const [fields, setFields] = React.useState<T>(initial);

    const handleChange = React.useCallback((id: string, value: any) => {
        setFields(prev => ({ ...prev, [id]: value }));
    }, []);

    return [fields, handleChange];
}
