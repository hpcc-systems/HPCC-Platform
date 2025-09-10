import * as React from "react";
import type { IObserverHandle } from "@hpcc-js/util";
import { Activity } from "@hpcc-js/comms";
import { useCounter } from "./util";

export function useActivity() {

    const [activity, setActivity] = React.useState<Activity>();
    const [lastUpdate, setLastUpdate] = React.useState(Date.now());
    const [count, increment] = useCounter();

    React.useEffect(() => {
        const activity = Activity.attach({ baseUrl: "" });
        let active = true;
        let handle: IObserverHandle | undefined;
        activity.lazyRefresh().then(() => {
            if (active) {
                setActivity(activity);
                handle = activity.watch(() => {
                    setLastUpdate(Date.now());
                });
            }
        });
        return () => {
            active = false;
            handle?.release();
        };
    }, [count]);

    return { activity, lastUpdate, refresh: increment };
}

