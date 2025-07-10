import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { globalKeyValStore, IKeyValStore, localKeyValStore, sessionKeyValStore, userKeyValStore } from "src/KeyValStore";
import { parseHash } from "../util/history";

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
                return (isNaN(numericValue) ? defaultValue : numericValue) as T;
            case "boolean":
                return (value === "true" ? true : false) as T;
            case "object":
                return JSON.parse(value);
            case "string":
            default:
                return value as T;
        }
    } catch (e) {
        return defaultValue;
    }
}

function useStore<T>(store: IKeyValStore, key: string, defaultValue: React.RefObject<T>, monitor: boolean = false): [value: T, setValue: (value: T) => Promise<void>, reset: () => Promise<void>] {

    const [value, setValue] = React.useState<T>();

    React.useEffect(() => {
        if (!store) return;
        store.get(key).then(value => {
            if (value === null) {
                setValue(defaultValue.current);
            } else {
                setValue(fromString<T>(value, defaultValue.current));
            }
        }).catch(e => {
            setValue(defaultValue.current);
        });
    }, [defaultValue, key, store]);

    React.useEffect(() => {
        if (!store || !monitor) return;
        const handle = store.monitor((messages) => {
            messages.filter(row => row.key === key).forEach(row => {
                setValue(fromString<T>(row.value, defaultValue.current));
            });
        });
        return () => handle.release();
    }, [defaultValue, key, monitor, store]);

    const extSetValue = React.useCallback((value: T) => {
        return store.set(key, toString<T>(value, defaultValue.current), monitor).then(() => {
            setValue(value);
        });
    }, [defaultValue, key, monitor, store]);

    const reset = React.useCallback(() => {
        return store.delete(key, monitor).then(() => {
            setValue(defaultValue.current);
        });
    }, [defaultValue, key, monitor, store]);

    return [value, extSetValue, reset];
}

export function useGlobalStore<T>(key: string, defaultValue: T, monitor: boolean = false) {
    const store = useConst(() => globalKeyValStore());
    const defaultValueRef = React.useRef(defaultValue);
    defaultValueRef.current = defaultValue;
    return useStore<T>(store, key, defaultValueRef, monitor);
}

export function useUserStore<T>(key: string, defaultValue: T, monitor: boolean = false) {
    const store = useConst(() => userKeyValStore());
    const defaultValueRef = React.useRef(defaultValue);
    defaultValueRef.current = defaultValue;
    return useStore<T>(store, key, defaultValueRef, monitor);
}

export function useLocalStore<T>(key: string, defaultValue: T, monitor: boolean = false) {
    const store = useConst(() => localKeyValStore());
    const defaultValueRef = React.useRef(defaultValue);
    defaultValueRef.current = defaultValue;
    return useStore<T>(store, key, defaultValueRef, monitor);
}

export function useSessionStore<T>(key: string, defaultValue: T, monitor: boolean = false) {
    const store = useConst(() => sessionKeyValStore());
    const defaultValueRef = React.useRef(defaultValue);
    defaultValueRef.current = defaultValue;
    return useStore<T>(store, key, defaultValueRef, monitor);
}

/*  Ephemeral Store 
    This store is used to persist data that is only needed for the current page.
    It is only persisted in a global variable.
    It is also non reactive = i.e. changing its content will not trigger a re-render.
*/

const g_state: Map<string, Map<string, Map<string, any>>> = new Map();

function useNonReactiveEphemeralPageGlobalStore<T>(): [Map<string, Map<string, T>>, () => void] {
    const pathname = useConst(() => parseHash(window.location.hash).pathname);

    const reset = React.useCallback(() => {
        g_state.set(pathname, new Map());
    }, [pathname]);

    if (!g_state.has(pathname)) {
        reset();
    }

    return [g_state.get(pathname), reset];
}

export function useNonReactiveEphemeralPageStore<T>(id: string): [Map<string, T>, () => void] {
    const [pageStates] = useNonReactiveEphemeralPageGlobalStore<T>();

    const reset = React.useCallback(() => {
        pageStates.set(id, new Map());
    }, [id, pageStates]);

    if (!pageStates.has(id)) {
        reset();
    }

    return [pageStates.get(id), reset];
}
