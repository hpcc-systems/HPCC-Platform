import * as React from "react";
import { ComponentDetails as ComponentDetailsWidget, Details as DetailsWidget, Summary as SummaryWidget } from "src/DiskUsage";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { pushUrl } from "../util/history";
import { ReflexContainer, ReflexElement, ReflexSplitter, classNames, styles } from "../layouts/react-reflex";

interface SummaryProps {
    cluster?: string;
}

export const Summary: React.FunctionComponent<SummaryProps> = ({
    cluster
}) => {
    const summary = React.useMemo(() => {
        const retVal = new SummaryWidget(cluster)
            .refresh(false)
            .on("click", (widget, details) => {
                pushUrl(`/clusters/${details.Name}/usage`);
            })
            ;
        return retVal;
    }, [cluster]);

    return <AutosizeHpccJSComponent widget={summary}></AutosizeHpccJSComponent >;
};

interface DetailsProps {
    cluster: string;
}

export const Details: React.FunctionComponent<DetailsProps> = ({
    cluster
}) => {
    const summary = React.useMemo(() => {
        const retVal = new DetailsWidget(cluster)
            .refresh()
            .on("componentClick", component => {
                pushUrl(`/machines/${component}/usage`);
            })
            ;
        return retVal;
    }, [cluster]);

    return <AutosizeHpccJSComponent widget={summary}></AutosizeHpccJSComponent >;
};

interface MachineUsageProps {
    machine: string;
}

export const MachineUsage: React.FunctionComponent<MachineUsageProps> = ({
    machine
}) => {
    const summary = React.useMemo(() => {
        const retVal = new ComponentDetailsWidget(machine)
            .refresh()
            ;
        return retVal;
    }, [machine]);

    return <AutosizeHpccJSComponent widget={summary}></AutosizeHpccJSComponent >;
};

interface ClusterUsageProps {
    cluster: string;
}

export const ClusterUsage: React.FunctionComponent<ClusterUsageProps> = ({
    cluster
}) => {
    return <ReflexContainer orientation="horizontal">
        <ReflexElement minSize={100} size={100} style={{ overflow: "hidden" }}>
            <Summary cluster={cluster} />
        </ReflexElement>
        <ReflexSplitter style={styles.reflexSplitter}>
            <div className={classNames.reflexSplitterDiv}></div>
        </ReflexSplitter>
        <ReflexElement style={{ overflow: "hidden" }}>
            <Details cluster={cluster} />
        </ReflexElement>
    </ReflexContainer >;
};
