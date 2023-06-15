import * as React from "react";
import { Workunit, Query } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { debounce, singletonDebounce } from "../util/throttle";
import { useCounter } from "./util";

const logger = scopedLogger("../hooks/query.ts");

export type UseQueryResponse = [Query, number, () => Promise<Query>];

export function useQuery(querySet: string, queryId: string): UseQueryResponse {

    const [retVal, setRetVal] = React.useState<UseQueryResponse>([undefined, Date.now(), () => Promise.resolve(undefined)]);

    React.useEffect(() => {
        if (querySet === undefined || querySet === null || queryId === undefined || queryId === null) {
            setRetVal([undefined, Date.now(), () => Promise.resolve(undefined)]);
            return;
        }
        const query = Query.attach({ baseUrl: "" }, querySet, queryId);
        const refresh = singletonDebounce(query, "refresh");

        let active = true;
        let handle;
        refresh().then(() => {
            if (active) {
                handle = query.watch(() => {
                    setRetVal([query, Date.now(), refresh]);
                });
            }
        }).catch(err => logger.error(err));

        return () => {
            active = false;
            handle?.release();
        };
    }, [querySet, queryId]);

    return retVal;
}

const fetchSnapshots = debounce((Cluster: string, Jobname: string, setSnapshots: React.Dispatch<React.SetStateAction<Workunit[]>>) => {
    Workunit.query({ baseUrl: "" }, { Cluster, Jobname }).then(snapshots => {
        setSnapshots(snapshots);
    }).catch(err => logger.error(err));
});

export function useQuerySnapshots(querySet: string, queryId: string): [Workunit[], () => void] {

    const [query, lastUpdate] = useQuery(querySet, queryId);
    const [snapshots, setSnapshots] = React.useState<Workunit[]>();
    const [count, inc] = useCounter();

    React.useEffect(() => {
        if (query?.QuerySet && query?.QueryName) {
            fetchSnapshots(query?.QuerySet, query?.QueryName, setSnapshots);
        }
    }, [query, lastUpdate, query?.QuerySet, query?.QueryName, count]);

    return [snapshots, inc];
}

