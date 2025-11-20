import * as React from "react";
import { Dropdown, IStackItemStyles, IStackStyles, IStackTokens, Overlay, Spinner, SpinnerSize, Text } from "@fluentui/react";
import { StackShim, StackItemShim } from "@fluentui/react-migration-v8-v9";
import { useConst } from "@fluentui/react-hooks";
import { Card, CardHeader, CardPreview } from "@fluentui/react-components";
import { WorkunitsService, WsWorkunits } from "@hpcc-js/comms";
import { Area, Column, Pie, Bar } from "@hpcc-js/chart";
import { chain, filter, group, map, sort } from "@hpcc-js/dataflow";
import nlsHPCC from "src/nlsHPCC";
import { wuidToDate } from "src/Utility";
import { Chip } from "./controls/Chip";
import { CreateWUQueryStore } from "../comms/workunit";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { pushParamExact } from "../util/history";
import { Workunits } from "./Workunits";

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

const DEFAULT_LASTNDAYS = 7;

const service = new WorkunitsService({ baseUrl: "" });

interface WorkunitEx extends WsWorkunits.ECLWorkunit {
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
        lastNDays: DEFAULT_LASTNDAYS,
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
            setWorkunits([...map(response.Workunits.ECLWorkunit, (row: WsWorkunits.ECLWorkunit) => ({ ...row, Day: wuidToDate(row.Wuid) }))]);
            setLoading(false);
        });
    }, [filterProps.lastNDays]);

    //  Cluster Chart ---
    const clusterChart = useConst(() =>
        new Bar()
            .columns(["Cluster", "Count"])
            .on("click", (row, col, sel) => pushParamExact("cluster", sel ? row.Cluster : undefined))
    );

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
    const ownerChart = useConst(() =>
        new Column()
            .columns(["Owner", "Count"])
            .on("click", (row, col, sel) => pushParamExact("owner", sel ? row.Owner : undefined))
    );

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
    const stateChart = useConst(() =>
        new Pie()
            .columns(["State", "Count"])
            .on("click", (row, col, sel) => pushParamExact("state", sel ? row.State : undefined))
    );

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
    const protectedChart = useConst(() =>
        new Pie()
            .columns(["Protected", "Count"])
            .on("click", (row, col, sel) => pushParamExact("protected", sel ? row.Protected === "true" : undefined))
    );

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
    const dayChart = useConst(() =>
        new Area()
            .columns(["Day", "Count"])
            .xAxisType("time")
            .interpolate("cardinal")
            // .xAxisTypeTimePattern("")
            .on("click", (row, col, sel) => pushParamExact("day", sel ? row.Day : undefined))
    );

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
    const workunitsStore = useConst(() => CreateWUQueryStore());

    const workunitsFilter = React.useMemo(() => {
        let start = new Date();
        let end = new Date();

        if (filterProps.day) {
            start = new Date(filterProps.day);
            end = new Date(filterProps.day);
            end.setDate(end.getDate() + 1);
        } else {
            const lastNDays = filterProps.lastNDays ?? DEFAULT_LASTNDAYS;
            start.setDate(start.getDate() - lastNDays);
        }

        return {
            StartDate: start.toISOString(),
            EndDate: end.toISOString(),
            ...(filterProps.cluster && { Cluster: filterProps.cluster }),
            ...(filterProps.owner && { Owner: filterProps.owner }),
            ...(filterProps.state && { State: filterProps.state }),
            ...(filterProps.protected !== undefined && { Protected: filterProps.protected }),
        };
    }, [filterProps.cluster, filterProps.day, filterProps.lastNDays, filterProps.owner, filterProps.protected, filterProps.state]);

    return <>
        <StackShim tokens={outerStackTokens} styles={{ root: { height: "100%" } }}>
            <StackShim styles={stackStyles} tokens={innerStackTokens}>
                <StackItemShim styles={stackItemStyles}>
                    <StackShim horizontal tokens={{ childrenGap: 16 }}  >
                        <StackItemShim align="start" styles={{ root: { width: "25%", height: "100%" } }}>
                            <Card>
                                <CardHeader header={
                                    <StackShim horizontal horizontalAlign="space-between">
                                        <Text variant="large" nowrap block styles={{ root: { fontWeight: "bold" } }}>{nlsHPCC.State}</Text>
                                        {filterProps.state !== undefined && <Chip label={filterProps.state} onDelete={() => pushParamExact("state", undefined)} />}
                                    </StackShim>
                                } />
                                <CardPreview>
                                    <AutosizeHpccJSComponent widget={stateChart} fixedHeight="240px" />
                                </CardPreview>
                            </Card>
                        </StackItemShim>
                        <StackItemShim align="center" styles={{ root: { width: "50%" } }}>
                            <Card  >
                                <CardHeader header={
                                    <StackShim horizontal horizontalAlign="space-between">
                                        <Text variant="large" nowrap block styles={{ root: { fontWeight: "bold" } }}>{nlsHPCC.Day}</Text>
                                        {filterProps.day !== undefined && <Chip label={filterProps.day} color="primary" onDelete={() => pushParamExact("day", undefined)} />}
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
                                    </StackShim>
                                } />
                                <CardPreview>
                                    <AutosizeHpccJSComponent widget={dayChart} fixedHeight="240px" />
                                </CardPreview>
                            </Card>
                        </StackItemShim>
                        <StackItemShim align="end" styles={{ root: { width: "25%" } }}>
                            <Card >
                                <CardHeader header={
                                    <StackShim horizontal horizontalAlign="space-between">
                                        <Text variant="large" nowrap block styles={{ root: { fontWeight: "bold" } }}>{nlsHPCC.Protected}</Text>
                                        {filterProps.protected !== undefined && <Chip label={"" + filterProps.protected} color="primary" onDelete={() => pushParamExact("protected", undefined)} />}
                                    </StackShim>
                                } />
                                <CardPreview>
                                    <AutosizeHpccJSComponent widget={protectedChart} fixedHeight="240px" />
                                </CardPreview>
                            </Card>
                        </StackItemShim>
                    </StackShim>
                </StackItemShim>
                <StackItemShim styles={stackItemStyles}>
                    <StackShim horizontal tokens={{ childrenGap: 16 }} >
                        <StackItemShim align="start" styles={{ root: { width: "66%" } }}>
                            <Card >
                                <CardHeader header={
                                    <StackShim horizontal horizontalAlign="space-between">
                                        <Text variant="large" nowrap block styles={{ root: { fontWeight: "bold" } }}>{nlsHPCC.Owner}</Text>
                                        {filterProps.owner !== undefined && <Chip label={filterProps.owner} color="primary" onDelete={() => pushParamExact("owner", undefined)} />}
                                    </StackShim>
                                } />
                                <CardPreview>
                                    <AutosizeHpccJSComponent widget={ownerChart} fixedHeight="240px" />
                                </CardPreview>
                            </Card>
                        </StackItemShim>
                        <StackItemShim align="center" styles={{ root: { width: "34%" } }}>
                            <Card>
                                <CardHeader header={
                                    <StackShim horizontal horizontalAlign="space-between">
                                        <Text variant="large" nowrap block styles={{ root: { fontWeight: "bold" } }}>{nlsHPCC.Cluster}</Text>
                                        {filterProps.cluster !== undefined && <Chip label={filterProps.cluster} color="primary" onDelete={() => pushParamExact("cluster", undefined)} />}
                                    </StackShim>
                                } />
                                <CardPreview>
                                    <AutosizeHpccJSComponent widget={clusterChart} fixedHeight="240px" />
                                </CardPreview>
                            </Card>
                        </StackItemShim>
                    </StackShim>
                </StackItemShim>
                <StackItemShim grow={5} styles={stackItemStyles}>
                    <Card style={{ height: "100%" }}>
                        <CardPreview style={{ height: "100%" }}>
                            <Workunits filter={workunitsFilter} store={workunitsStore} />
                        </CardPreview>
                    </Card>
                </StackItemShim>
            </StackShim>
        </StackShim>
        {loading && <Overlay styles={{ root: { display: "flex", justifyContent: "center" } }}>
            <Spinner label={nlsHPCC.Loading} size={SpinnerSize.large} />
        </Overlay>}
    </>;
};