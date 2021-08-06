import * as React from "react";
import { LogicalFile } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import * as WsDfu from "src/WsDfu";
import { useCounter } from "./Workunit";

const logger = scopedLogger("src-react/hooks/file.ts");

export function useFile(cluster: string, name: string): [LogicalFile, number, () => void] {

    const [file, setFile] = React.useState<LogicalFile>();
    const [lastUpdate, setLastUpdate] = React.useState(Date.now());
    const [count, increment] = useCounter();

    React.useEffect(() => {
        const file = LogicalFile.attach({ baseUrl: "" }, cluster, name);
        file.fetchInfo().then(response => {
            setFile(file);
            setLastUpdate(Date.now());
        }).catch(logger.error);
    }, [cluster, name, count]);

    return [file, lastUpdate, increment];
}

export function useDefFile(cluster: string, name: string, format: "def" | "xml"): [string] {
    const [file, setFile] = React.useState("");

    React.useEffect(() => {
        if (name) {
            WsDfu.DFUDefFile(
                { "request": { "Name": name, "Format": format }
            }).then(response => {
                setFile(response);
            }).catch(logger.error);
        }
    }, [cluster, format, name]);

    return [file];
}
