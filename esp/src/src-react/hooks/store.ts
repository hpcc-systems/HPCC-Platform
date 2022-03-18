import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { globalKeyValStore, IKeyValStore, userKeyValStore } from "src/KeyValStore";

function useStore(store: IKeyValStore, key: string, defaultValue?: string, monitor: boolean = false): [value: string, setValue: (value: string) => void, reset: () => void] {

    const [value, setValue] = React.useState<string>();

    React.useEffect(() => {
        if (!store) return;
        store.get(key).then(value => {
            setValue(typeof value !== "string" ? defaultValue : value);
        }).catch(e => {
            setValue(defaultValue);
        });
    }, [defaultValue, key, store]);

    React.useEffect(() => {
        if (!store || !monitor) return;
        const handle = store.monitor((messages) => {
            setValue(messages[0].value);
        });
        return () => handle.release();
    }, [store, monitor]);

    const extSetValue = useConst(() => {
        return (value: string) => {
            store.set(key, value, monitor).then(() => {
                setValue(value);
            });
        };
    });

    const reset = useConst(() => {
        return () => {
            store.delete(key, monitor).then(() => {
                setValue(defaultValue);
            });
        };
    });

    return [value, extSetValue, reset];
}

export function useUserStore(key: string, defaultValue?: string, monitor: boolean = false) {

    const store = useConst(() => userKeyValStore());
    return useStore(store, key, defaultValue, monitor);
}

export function useGlobalStore(key: string, defaultValue?: string, monitor: boolean = false) {

    const store = useConst(() => globalKeyValStore());
    return useStore(store, key, defaultValue, monitor);
}