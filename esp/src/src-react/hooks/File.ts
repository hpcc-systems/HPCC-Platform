import * as React from "react";
import { LogicalFile } from "@hpcc-js/comms";
import { useCounter } from "./Workunit";

export function useFile(cluster: string, name: string): [LogicalFile, number, () => void] {

    const [file, setFile] = React.useState<LogicalFile>();
    const [lastUpdate, setLastUpdate] = React.useState(Date.now());
    const [count, increment] = useCounter();

    React.useEffect(() => {
        const file = LogicalFile.attach({ baseUrl: "" }, cluster, name);
        file.fetchInfo().then(response => {
            setFile(file);
            setLastUpdate(Date.now());
        });
    }, [cluster, name, count]);

    return [file, lastUpdate, increment];
}

