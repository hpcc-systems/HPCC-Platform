import * as React from "react";
import { LogicalFile, WsDfu } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { singletonDebounce } from "../util/throttle";
import { useCounter } from "./util";

const logger = scopedLogger("../hooks/file.ts");

export function useFile(cluster: string, name: string): [LogicalFile, boolean, number, () => void] {

    const [file, setFile] = React.useState<LogicalFile>();
    const [isProtected, setIsProtected] = React.useState(false);
    const [lastUpdate, setLastUpdate] = React.useState(Date.now());
    const [count, increment] = useCounter();

    React.useEffect(() => {
        const file = LogicalFile.attach({ baseUrl: "" }, cluster === "undefined" ? undefined : cluster, name);
        let active = true;
        let handle;
        const fetchInfo = singletonDebounce(file, "fetchInfo");
        fetchInfo()
            .then((response) => {
                if (active) {
                    setFile(file);
                    setIsProtected(response.ProtectList?.DFUFileProtect?.length > 0 || false);
                    setLastUpdate(Date.now());
                    handle = file.watch(() => {
                        setIsProtected(response.ProtectList?.DFUFileProtect?.length > 0 || false);
                        setLastUpdate(Date.now());
                    });
                }
            })
            .catch(err => logger.error(err))
            ;
        return () => {
            active = false;
            handle?.release();
        };
    }, [cluster, count, name]);

    return [file, isProtected, lastUpdate, increment];
}

export function useDefFile(cluster: string, name: string, format: WsDfu.DFUDefFileFormat): [string, () => void] {

    const [file] = useFile(cluster, name);
    const [defFile, setDefFile] = React.useState("");
    const [count, increment] = useCounter();

    React.useEffect(() => {
        if (file) {
            file.fetchDefFile(format)
                .then(setDefFile)
                .catch(err => logger.error(err))
                ;
        }
    }, [file, format, count]);

    return [defFile, increment];
}

export function useFileHistory(cluster: string, name: string): [WsDfu.Origin[], () => void, () => void] {

    const [file] = useFile(cluster, name);
    const [history, setHistory] = React.useState<WsDfu.Origin[]>([]);
    const [count, increment] = useCounter();

    const eraseHistory = React.useCallback(() => {
        file?.eraseHistory()
            .then(response => {
                setHistory(response);
            })
            .catch(err => logger.error(err))
            ;
    }, [file]);

    React.useEffect(() => {
        if (file) {
            file.fetchListHistory()
                .then(response => {
                    setHistory(response);
                })
                .catch(err => logger.error(err))
                ;
        }
    }, [file, count]);

    return [history, eraseHistory, increment];
}

export function useSubfiles(cluster: string, name: string): [WsDfu.subfiles, () => void] {

    const [file] = useFile(cluster, name);
    const [subfiles, setSubfiles] = React.useState<WsDfu.subfiles>({ Item: [] });
    const [count, increment] = useCounter();

    React.useEffect(() => {
        if (file) {
            file.fetchInfo()
                .then(response => {
                    setSubfiles(response.subfiles ?? { Item: [] });
                })
                .catch(err => logger.error(err))
                ;
        }
    }, [file, count]);

    return [subfiles, increment];
}
