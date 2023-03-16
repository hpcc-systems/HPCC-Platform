import * as React from "react";
import { WsResources, ResourcesService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { containerized } from "src/BuildInfo";
import { useCounter } from "./workunit";

const logger = scopedLogger("../hooks/resources.ts");

const service = new ResourcesService({ baseUrl: "" });

export function useWebLinks(): [WsResources.DiscoveredWebLink[], () => void] {

    const [webLinks, setWebLinks] = React.useState<WsResources.DiscoveredWebLink[]>();
    const [count, increment] = useCounter();

    React.useEffect(() => {
        if (containerized) {
            service.WebLinksQuery({}).then(response => {
                setWebLinks(response?.DiscoveredWebLinks?.DiscoveredWebLink);
            }).catch(err => logger.error(err));
        }
    }, [count]);

    return [webLinks, increment];
}

export function useServices(): [WsResources.Service[], () => void] {

    const [services, setServices] = React.useState<WsResources.Service[]>([]);
    const [count, increment] = useCounter();

    React.useEffect(() => {
        if (containerized) {
            service.ServiceQuery({}).then(response => {
                setServices(response?.Services?.Service);
            }).catch(err => logger.error(err));
        }
    }, [count]);

    return [services, increment];
}
