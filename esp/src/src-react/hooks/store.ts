import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { userKeyValStore } from "src/KeyValStore";

export function useUserStore(key: string, defaultValue?: string, monitor: boolean = false): [value: string, setValue: (value: string) => void] {

    const store = useConst(() => userKeyValStore());
    const [value, setValue] = React.useState<string>();

    React.useEffect(() => {
        if (!store) return;
        store.get(key).then(value => {
            setValue(value === undefined ? defaultValue : value);
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

    return [value, extSetValue];
}
