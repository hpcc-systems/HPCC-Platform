import * as React from "react";
import { Dropdown, IStackItemStyles, IStackStyles, IStackTokens, Overlay, Spinner, SpinnerSize, Stack, Text } from "@fluentui/react";
import { Card } from "@fluentui/react-cards";
import * as Observable from "dojo/store/Observable";
import * as ESPWorkunit from "src/ESPWorkunit";
import { WorkunitsService, WUQuery } from "@hpcc-js/comms";
import { Area, Column, Pie, Bar } from "@hpcc-js/chart";
import { chain, filter, group, map, sort } from "@hpcc-js/dataflow";
import Chip from "@material-ui/core/Chip";
import nlsHPCC from "src/nlsHPCC";
import { Memory } from "src/Memory";
import { pushParamExact } from "../util/history";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { Workunits } from "./Workunits";
import { useConst } from "@fluentui/react-hooks";

const service = new WorkunitsService({ baseUrl: "" });

const wuidToDate = (wuid: string) => `${wuid.substr(1, 4)}-${wuid.substr(5, 2)}-${wuid.substr(7, 2)}`;

interface WorkunitEx extends WUQuery.ECLWorkunit {
    Day: string;
}

export interface WorkunitsDashboardFilter {
    lastNDays?: number;
    cluster?: string;
    owner?: string;
    state?: string;
    protected?: boolean;
    day?: string;
}

export interface WorkunitsDashboardProps {
    filterProps?: WorkunitsDashboardFilter;
}

export const WorkunitsDashboard: React.FunctionComponent<WorkunitsDashboardProps> = ({
    filterProps
}) => {
    filterProps = {
        lastNDays: 7,
        ...filterProps
    };

    const [loading, setLoading] = React.useState(false);
    const [workunits, setWorkunits] = React.useState<WorkunitEx[]>([]);

    React.useEffect(() => {
        setLoading(true);
        setWorkunits([]);
        const end = new Date();
        const start = new Date();
        start.setDate(start.getDate() - filterProps.lastNDays);
        service.WUQuery({
            StartDate: start.toISOString(),
            EndDate: end.toISOString(),
            PageSize: 999999
        }).then(response => {
            setWorkunits([...map(response.Workunits.ECLWorkunit, (row: WUQuery.ECLWorkunit) => ({ ...row, Day: wuidToDate(row.Wuid) }))]);
            setLoading(false);
        });
    }, [filterProps.lastNDays]);

    //  Cluster Chart ---
    const clusterChart = React.useRef(
        new Bar()
            .columns(["Cluster", "Count"])
            .on("click", (row, col, sel) => pushParamExact("cluster", sel ? row.Cluster : undefined))
    ).current;

    const clusterPipeline = chain(
        filter<WorkunitEx>(row => filterProps.state === undefined || row.State === filterProps.state),
        filter(row => filterProps.owner === undefined || row.Owner === filterProps.owner),
        filter(row => filterProps.day === undefined || row.Day === filterProps.day),
        filter(row => filterProps.protected === undefined || row.Protected === filterProps.protected),
        group(row => row.Cluster),
        map(row => [row.key, row.value.length] as [string, number]),
        sort((l, r) => l[0].localeCompare(r[0])),
    );

    clusterChart
        .data([...clusterPipeline(workunits)])
        ;

    //  Owner Chart ---
    const ownerChart = React.useRef(
        new Column()
            .columns(["Owner", "Count"])
            .on("click", (row, col, sel) => pushParamExact("owner", sel ? row.Owner : undefined))
    ).current;

    const ownerPipeline = chain(
        filter<WorkunitEx>(row => filterProps.cluster === undefined || row.Cluster === filterProps.cluster),
        filter(row => filterProps.state === undefined || row.State === filterProps.state),
        filter(row => filterProps.day === undefined || row.Day === filterProps.day),
        filter(row => filterProps.protected === undefined || row.Protected === filterProps.protected),
        group(row => row.Owner),
        map(row => [row.key, row.value.length] as [string, number]),
        sort((l, r) => l[0].localeCompare(r[0])),
    );

    ownerChart
        .data([...ownerPipeline(workunits)])
        ;

    //  State Chart ---
    const stateChart = React.useRef(
        new Pie()
            .columns(["State", "Count"])
            .on("click", (row, col, sel) => pushParamExact("state", sel ? row.State : undefined))
    ).current;

    const statePipeline = chain(
        filter<WorkunitEx>(row => filterProps.cluster === undefined || row.Cluster === filterProps.cluster),
        filter(row => filterProps.owner === undefined || row.Owner === filterProps.owner),
        filter(row => filterProps.day === undefined || row.Day === filterProps.day),
        filter(row => filterProps.protected === undefined || row.Protected === filterProps.protected),
        group(row => row.State),
        map(row => [row.key, row.value.length])
    );

    stateChart
        .data([...statePipeline(workunits)])
        ;

    //  Protected Chart ---
    const protectedChart = React.useRef(
        new Pie()
            .columns(["Protected", "Count"])
            .on("click", (row, col, sel) => pushParamExact("protected", sel ? row.Protected === "true" : undefined))
    ).current;

    const protectedPipeline = chain(
        filter<WorkunitEx>(row => filterProps.cluster === undefined || row.Cluster === filterProps.cluster),
        filter(row => filterProps.owner === undefined || row.Owner === filterProps.owner),
        filter(row => filterProps.day === undefined || row.Day === filterProps.day),
        group(row => "" + row.Protected),
        map(row => [row.key, row.value.length])
    );

    protectedChart
        .data([...protectedPipeline(workunits)])
        ;

    //  Day Chart ---
    const dayChart = React.useRef(
        new Area()
            .columns(["Day", "Count"])
            .xAxisType("time")
            .interpolate("cardinal")
            // .xAxisTypeTimePattern("")
            .on("click", (row, col, sel) => pushParamExact("day", sel ? row.Day : undefined))
    ).current;

    const dayPipeline = chain(
        filter<WorkunitEx>(row => filterProps.cluster === undefined || row.Cluster === filterProps.cluster),
        filter(row => filterProps.owner === undefined || row.Owner === filterProps.owner),
        filter(row => filterProps.state === undefined || row.State === filterProps.state),
        filter(row => filterProps.protected === undefined || row.Protected === filterProps.protected),
        group(row => row.Day),
        map(row => [row.key, row.value.length] as [string, number]),
        sort((l, r) => l[0].localeCompare(r[0])),
    );

    dayChart
        .data([...dayPipeline(workunits)])
        ;

    //  Table ---
    const workunitsStore = useConst(new Observable(new Memory({ idProperty: "Wuid", data: [] })));
    const tablePipeline = chain(
        filter<WorkunitEx>(row => filterProps.cluster === undefined || row.Cluster === filterProps.cluster),
        filter(row => filterProps.owner === undefined || row.Owner === filterProps.owner),
        filter(row => filterProps.state === undefined || row.State === filterProps.state),
        filter(row => filterProps.protected === undefined || row.Protected === filterProps.protected),
        filter(row => filterProps.day === undefined || row.Day === filterProps.day),
        map(row => ESPWorkunit.Get(row.Wuid, row))
    );
    workunitsStore.setData([...tablePipeline(workunits)]);

    //  --- --- ---
    const stackStyles: IStackStyles = {
        root: {
            height: "100%",
        },
    };
    const stackItemStyles: IStackItemStyles = {
        root: {
            minHeight: 240
        },
    };
    const outerStackTokens: IStackTokens = { childrenGap: 5 };
    const innerStackTokens: IStackTokens = {
        childrenGap: 5,
        padding: 10,
    };

    return <>
        <Stack tokens={outerStackTokens} styles={{ root: { height: "100%" } }}>
            <Stack styles={stackStyles} tokens={innerStackTokens}>
                <Stack.Item styles={stackItemStyles}>
                    <Stack horizontal tokens={{ childrenGap: 16 }}  >
                        <Stack.Item align="start" styles={{ root: { width: "25%", height: "100%" } }}>
                            <Card tokens={{ childrenMargin: 12, minWidth: "100%", minHeight: "100%" }}>
                                <Card.Item>
                                    <Stack horizontal horizontalAlign="space-between">
                                        <Text variant="large" nowrap block styles={{ root: { fontWeight: "bold" } }}>{nlsHPCC.State}</Text>
                                        {filterProps.state !== undefined && <Chip label={filterProps.state} clickable color="primary" onDelete={() => pushParamExact("state", undefined)} />}
                                    </Stack>
                                </Card.Item>
                                <Card.Item>
                                    <AutosizeHpccJSComponent widget={stateChart} fixedHeight="240px" />
                                </Card.Item>
                            </Card>
                        </Stack.Item>
                        <Stack.Item align="center" styles={{ root: { width: "50%" } }}>
                            <Card tokens={{ childrenMargin: 12, minWidth: "100%" }} >
                                <Card.Item>
                                    <Stack horizontal horizontalAlign="space-between">
                                        <Text variant="large" nowrap block styles={{ root: { fontWeight: "bold" } }}>{nlsHPCC.Day}</Text>
                                        {filterProps.day !== undefined && <Chip label={filterProps.day} clickable color="primary" onDelete={() => pushParamExact("day", undefined)} />}
                                        <Dropdown onChange={(evt, opt, idx) => { pushParamExact("lastNDays", opt.key); }}
                                            options={[
                                                { key: 1, text: "1 Day", selected: filterProps.lastNDays === 1 },
                                                { key: 2, text: "2 Days", selected: filterProps.lastNDays === 2 },
                                                { key: 3, text: "3 Days", selected: filterProps.lastNDays === 3 },
                                                { key: 7, text: "1 Week", selected: filterProps.lastNDays === 7 },
                                                { key: 14, text: "2 Weeks", selected: filterProps.lastNDays === 14 },
                                                { key: 21, text: "3 Weeks", selected: filterProps.lastNDays === 21 },
                                                { key: 31, text: "1 Month", selected: filterProps.lastNDays === 31 }
                                            ]}
                                        />
                                    </Stack>
                                </Card.Item>
                                <Card.Item>
                                    <AutosizeHpccJSComponent widget={dayChart} fixedHeight="240px" />
                                </Card.Item>
                            </Card>
                        </Stack.Item>
                        <Stack.Item align="end" styles={{ root: { width: "25%" } }}>
                            <Card tokens={{ childrenMargin: 12, minWidth: "100%" }}>
                                <Card.Item>
                                    <Stack horizontal horizontalAlign="space-between">
                                        <Text variant="large" nowrap block styles={{ root: { fontWeight: "bold" } }}>{nlsHPCC.Protected}</Text>
                                        {filterProps.protected !== undefined && <Chip label={"" + filterProps.protected} clickable color="primary" onDelete={() => pushParamExact("protected", undefined)} />}
                                    </Stack>
                                </Card.Item>
                                <Card.Item>
                                    <AutosizeHpccJSComponent widget={protectedChart} fixedHeight="240px" />
                                </Card.Item>
                            </Card>
                        </Stack.Item>
                    </Stack>
                </Stack.Item>
                <Stack.Item styles={stackItemStyles}>
                    <Stack horizontal tokens={{ childrenGap: 16 }} >
                        <Stack.Item align="start" styles={{ root: { width: "66%" } }}>
                            <Card tokens={{ childrenMargin: 12, minWidth: "100%" }}>
                                <Card.Item>
                                    <Stack horizontal horizontalAlign="space-between">
                                        <Text variant="large" nowrap block styles={{ root: { fontWeight: "bold" } }}>{nlsHPCC.Owner}</Text>
                                        {filterProps.owner !== undefined && <Chip label={filterProps.owner} clickable color="primary" onDelete={() => pushParamExact("owner", undefined)} />}
                                    </Stack>
                                </Card.Item>
                                <Card.Item>
                                    <AutosizeHpccJSComponent widget={ownerChart} fixedHeight="240px" />
                                </Card.Item>
                            </Card>
                        </Stack.Item>
                        <Stack.Item align="center" styles={{ root: { width: "34%" } }}>
                            <Card tokens={{ childrenMargin: 12, minWidth: "100%" }} >
                                <Card.Item>
                                    <Stack horizontal horizontalAlign="space-between">
                                        <Text variant="large" nowrap block styles={{ root: { fontWeight: "bold" } }}>{nlsHPCC.Cluster}</Text>
                                        {filterProps.cluster !== undefined && <Chip label={filterProps.cluster} clickable color="primary" onDelete={() => pushParamExact("cluster", undefined)} />}
                                    </Stack>
                                </Card.Item>
                                <Card.Item>
                                    <AutosizeHpccJSComponent widget={clusterChart} fixedHeight="240px" />
                                </Card.Item>
                            </Card>
                        </Stack.Item>
                    </Stack>
                </Stack.Item>
                <Stack.Item grow={5} styles={stackItemStyles}>
                    <Card tokens={{ childrenMargin: 4, minWidth: "100%", height: "100%" }}>
                        <Card.Section tokens={{}} styles={{ root: { height: "100%" } }}>
                            <Workunits store={workunitsStore} />
                        </Card.Section>
                    </Card>
                </Stack.Item>
            </Stack>
        </Stack>
        {loading && <Overlay styles={{ root: { display: "flex", justifyContent: "center" } }}>
            <Spinner label={nlsHPCC.Loading} size={SpinnerSize.large} />
        </Overlay>}
    </>;
};