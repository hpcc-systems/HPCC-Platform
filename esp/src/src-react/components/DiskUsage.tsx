import * as React from "react";
import { Link } from "@fluentui/react";
import { MachineService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { ComponentDetails as ComponentDetailsWidget, Summary as SummaryWidget } from "src/DiskUsage";
import nlsHPCC from "src/nlsHPCC";
import * as Utility from "src/Utility";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { ReflexContainer, ReflexElement, ReflexSplitter, classNames, styles } from "../layouts/react-reflex";
import { pushUrl } from "../util/history";
import { FluentGrid, useFluentStoreState } from "./controls/Grid";

const logger = scopedLogger("src-react/components/DiskUsage.tsx");

const machineService = new MachineService({ baseUrl: "" });

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
                pushUrl(`/operations/clusters/${details.Name}/usage`);
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

    const { refreshTable } = useFluentStoreState({});

    //  Grid ---
    const columns = React.useMemo(() => {
        return {
            PercentUsed: {
                label: nlsHPCC.PercentUsed, width: 50, formatter: (percent) => {
                    let className = "";

                    if (percent <= 70) { className = "bgFilled bgGreen"; }
                    else if (percent > 70 && percent < 80) { className = "bgFilled bgOrange"; }
                    else { className = "bgFilled bgRed"; }

                    return <span className={className}>{percent}</span>;
                }
            },
            Component: { label: nlsHPCC.Component, width: 90 },
            Type: { label: nlsHPCC.Type, width: 40 },
            IPAddress: {
                label: nlsHPCC.IPAddress, width: 140,
                formatter: (ip) => <Link href={`#/operations/machines/${ip}/usage`}>{ip}</Link>
            },
            Path: { label: nlsHPCC.Path, width: 220 },
            InUse: { label: nlsHPCC.InUse, width: 50 },
            Total: { label: nlsHPCC.Total, width: 50 },
        };
    }, []);

    type Columns = typeof columns;
    type Row = { __hpcc_id: string } & { [K in keyof Columns]: string | number };
    const [data, setData] = React.useState<Row[]>([]);

    const refreshData = React.useCallback(() => {
        machineService.GetTargetClusterUsageEx([cluster])
            .then(response => {
                const _data: Row[] = [];
                if (response) {
                    response.forEach(component => {
                        component.ComponentUsages.forEach(cu => {
                            cu.MachineUsages.forEach(mu => {
                                mu.DiskUsages.forEach((du, i) => {
                                    _data.push({
                                        __hpcc_id: `__usage_${i}`,
                                        PercentUsed: Math.round((du.InUse / du.Total) * 100),
                                        Component: cu.Name,
                                        IPAddress: mu.Name,
                                        Type: du.Name,
                                        Path: du.Path,
                                        InUse: Utility.convertedSize(du.InUse),
                                        Total: Utility.convertedSize(du.Total)
                                    });
                                });
                            });
                        });
                    });
                }
                setData(_data);
            })
            .catch(err => logger.error(err))
            ;
    }, [cluster]);

    React.useEffect(() => {
        refreshData();
    }, [refreshData]);

    return <FluentGrid
        data={data}
        primaryID={"__hpcc_id"}
        sort={{ attribute: "__hpcc_id", descending: false }}
        columns={columns}
        setSelection={() => null}
        setTotal={() => null}
        refresh={refreshTable}
    ></FluentGrid>;
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
