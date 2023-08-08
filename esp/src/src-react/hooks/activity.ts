import * as React from "react";
import { Activity } from "@hpcc-js/comms";
import { useCounter } from "./util";

export function useActivity(): [Activity, number, () => void] {

    const [activity, setActivity] = React.useState<Activity>();
    const [lastUpdate, setLastUpdate] = React.useState(Date.now());
    const [count, increment] = useCounter();

    React.useEffect(() => {
        const activity = Activity.attach({ baseUrl: "" });
        let active = true;
        activity.lazyRefresh().then(() => {
            if (active) {
                setActivity(activity);
                setLastUpdate(Date.now());
            }
        });
        return () => {
            active = false;
        };
    }, [count]);

    return [activity, lastUpdate, increment];
}
