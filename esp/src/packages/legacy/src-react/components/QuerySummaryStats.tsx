import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "@fluentui/react";
import { Query } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";

const logger = scopedLogger("src-react/components/QuerySummaryStats.tsx");

interface QuerySummaryStatsProps {
    querySet: string;
    queryId: string;
}

export const QuerySummaryStats: React.FunctionComponent<QuerySummaryStatsProps> = ({
    querySet,
    queryId
}) => {

    const query = React.useMemo(() => {
        return Query.attach({ baseUrl: "" }, querySet, queryId);
    }, [querySet, queryId]);
    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            Endpoint: { label: nlsHPCC.EndPoint, width: 72, sortable: true },
            Status: { label: nlsHPCC.Status, width: 72, sortable: true },
            StartTime: { label: nlsHPCC.StartTime, width: 160, sortable: true },
            EndTime: { label: nlsHPCC.EndTime, width: 160, sortable: true },
            CountTotal: { label: nlsHPCC.CountTotal, width: 88, sortable: true },
            CountFailed: { label: nlsHPCC.CountFailed, width: 80, sortable: true },
            AverageBytesOut: { label: nlsHPCC.MeanBytesOut, width: 80, sortable: true },
            SizeAvgPeakMemory: { label: nlsHPCC.SizeMeanPeakMemory, width: 88, sortable: true },
            TimeAvgTotalExecuteMinutes: { label: nlsHPCC.TimeMeanTotalExecuteMinutes, width: 88, sortable: true },
            TimeMinTotalExecuteMinutes: { label: nlsHPCC.TimeMinTotalExecuteMinutes, width: 88, sortable: true },
            TimeMaxTotalExecuteMinutes: { label: nlsHPCC.TimeMaxTotalExecuteMinutes, width: 88, sortable: true },
            Percentile97: { label: nlsHPCC.Percentile97, width: 80, sortable: true },
            Percentile97Estimate: { label: nlsHPCC.Percentile97Estimate, sortable: true }
        };
    }, []);

    const refreshData = React.useCallback(() => {
        query?.fetchSummaryStats().then(({ StatsList }) => {
            if (StatsList?.QuerySummaryStats) {
                setData(StatsList?.QuerySummaryStats.map((item, idx) => {
                    return {
                        __hpcc_id: idx,
                        Endpoint: item.Endpoint,
                        Status: item.Status,
                        StartTime: item.StartTime,
                        EndTime: item.EndTime,
                        CountTotal: item.CountTotal,
                        CountFailed: item.CountFailed,
                        AverageBytesOut: item.AverageBytesOut,
                        SizeAvgPeakMemory: item.SizeAvgPeakMemory,
                        TimeAvgTotalExecuteMinutes: item.TimeAvgTotalExecuteMinutes,
                        TimeMinTotalExecuteMinutes: item.TimeMinTotalExecuteMinutes,
                        TimeMaxTotalExecuteMinutes: item.TimeMaxTotalExecuteMinutes,
                        Percentile97: item.Percentile97,
                        Percentile97Estimate: item.Percentile97Estimate
                    };
                }));
            }
        }).catch(err => logger.error(err));
    }, [query]);

    React.useEffect(() => {
        refreshData();
    }, [refreshData]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
    ], [refreshData]);

    const copyButtons = useCopyButtons(columns, selection, "querySummaryStats");

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={<FluentGrid
            data={data}
            primaryID={"__hpcc_id"}
            sort={{ attribute: "__hpcc_id", descending: false }}
            columns={columns}
            setSelection={setSelection}
            setTotal={setTotal}
            refresh={refreshTable}
        ></FluentGrid>}
    />;
};