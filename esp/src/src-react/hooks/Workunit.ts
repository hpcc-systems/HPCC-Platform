import * as React from "react";
import { Workunit, Result, WUStateID } from "@hpcc-js/comms";

export function useWorkunit(wuid: string): [Workunit, WUStateID] {

    const [workunit, setWorkunit] = React.useState<Workunit>();
    const [state, setState] = React.useState<WUStateID>();

    React.useEffect(() => {
        const wu = Workunit.attach({ baseUrl: "" }, wuid);
        const handle = wu.watch(() => {
            setState(wu.StateID);
        });
        setWorkunit(wu);
        return () => {
            handle.release();
        };
    }, [wuid]);

    return [workunit, state];
}

export function useWorkunitResults(wuid: string): [Result[], Workunit, WUStateID] {

    const [workunit, state] = useWorkunit(wuid);
    const [results, setResults] = React.useState<Result[]>([]);

    React.useEffect(() => {
        workunit?.fetchResults().then(results => {
            setResults(results);
        });
    }, [workunit, state]);

    return [results, workunit, state];
}
