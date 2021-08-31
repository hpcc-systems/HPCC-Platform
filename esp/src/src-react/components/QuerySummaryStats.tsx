import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { Query } from "@hpcc-js/comms";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { DojoGrid } from "./DojoGrid";

interface QuerySummaryStatsProps {
    querySet: string;
    queryId: string;
}

export const QuerySummaryStats: React.FunctionComponent<QuerySummaryStatsProps> = ({
    querySet,
    queryId
}) => {

    const [query, setQuery] = React.useState<any>();
    const [grid, setGrid] = React.useState<any>(undefined);

    //  Grid ---
    const gridStore = useConst(new Observable(new Memory("__hpcc_id")));
    const gridQuery = useConst({});
    const gridSort = useConst([{ attribute: "__hpcc_id" }]);
    const gridColumns = useConst({
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
    });

    const refreshTable = React.useCallback((clearSelection = false) => {
        grid?.set("query", gridQuery);
        if (clearSelection) {
            grid?.clearSelection();
        }
    }, [grid, gridQuery]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
        },
    ], [refreshTable]);

    React.useEffect(() => {
        setQuery(Query.attach({ baseUrl: "" }, querySet, queryId));
    }, [setQuery, queryId, querySet]);

    React.useEffect(() => {
        if (!query) return;
        query?.fetchSummaryStats().then(({ StatsList }) => {
            if (StatsList?.QuerySummaryStats) {
                gridStore.setData(StatsList?.QuerySummaryStats.map((item, idx) => {
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
                refreshTable();
            }
        });
    }, [gridStore, query, refreshTable]);

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} />}
        main={<DojoGrid store={gridStore} query={gridQuery} sort={gridSort} columns={gridColumns} setGrid={setGrid} setSelection={null} />}
    />;
};