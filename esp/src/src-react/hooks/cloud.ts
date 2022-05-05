import * as React from "react";
import { CloudService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import type { V1Pod } from "@kubernetes/client-node";

import { useCounter } from "./workunit";

const logger = scopedLogger("../hooks/cloud.ts");

const service = new CloudService({ baseUrl: "" });

export function usePods(): [V1Pod[], () => void] {

    const [retVal, setRetVal] = React.useState<V1Pod[]>([]);
    const [count, inc] = useCounter();

    React.useEffect(() => {
        service.getPODs().then(pods => {
            setRetVal(pods);
        }).catch(err => logger.error(err));
    }, [count]);

    return [retVal, inc];
}
