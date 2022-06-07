import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { globalKeyValStore, IKeyValStore, userKeyValStore } from "src/KeyValStore";

function toString<T>(value: T, defaultValue: T): string {
    if (value === undefined) value = defaultValue;
    switch (typeof defaultValue) {
        case "number":
            return isNaN(value as any) ? defaultValue.toString() : value.toString();
        case "boolean":
            return value ? "true" : "false";
        case "object":
            return JSON.stringify(value);
        case "string":
        default:
            return value as any;
    }
}

function fromString<T>(value: string, defaultValue: T): T {
    if (value === undefined) return defaultValue;
    try {
        switch (typeof defaultValue) {
            case "number":
                const numericValue = Number(value);
                return isNaN(numericValue) ? defaultValue : numericValue as any;
            case "boolean":
                return value === "true" ? true : false as any;
            case "object":
                return JSON.parse(value);
            case "string":
            default:
                return value as any;
        }
    } catch (e) {
        return defaultValue;
    }
}

function useStore<T>(store: IKeyValStore, key: string, defaultValue: T, monitor: boolean = false): [value: T, setValue: (value: T) => void, reset: () => void] {

    const [value, setValue] = React.useState<T>(defaultValue);

    React.useEffect(() => {
        if (!store) return;
        store.get(key).then(value => {
            setValue(fromString<T>(value, defaultValue));
        }).catch(e => {
            setValue(defaultValue);
        });
    }, [defaultValue, key, store]);

    React.useEffect(() => {
        if (!store || !monitor) return;
        const handle = store.monitor((messages) => {
            messages.filter(row => row.key === key).forEach(row => {
                setValue(fromString<T>(row.value, defaultValue));
            });
        });
        return () => handle.release();
    }, [defaultValue, key, monitor, store]);

    const extSetValue = React.useCallback((value: T) => {
        store.set(key, toString<T>(value, defaultValue), monitor).then(() => {
            setValue(value);
        });
    }, [defaultValue, key, monitor, store]);

    const reset = React.useCallback(() => {
        store.delete(key, monitor).then(() => {
            setValue(defaultValue);
        });
    }, [defaultValue, key, monitor, store]);

    return [value, extSetValue, reset];
}

export function useUserStore<T>(key: string, defaultValue: T, monitor: boolean = false) {

    const store = useConst(() => userKeyValStore());
    return useStore<T>(store, key, defaultValue, monitor);
}

export function useGlobalStore<T>(key: string, defaultValue: T, monitor: boolean = false) {

    const store = useConst(() => globalKeyValStore());
    return useStore<T>(store, key, defaultValue, monitor);
}
