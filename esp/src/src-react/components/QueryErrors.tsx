import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import * as ESPQuery from "src/ESPQuery";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { DojoGrid } from "./DojoGrid";

const logger = scopedLogger("../components/QueryErrors.tsx");

interface QueryErrorsProps {
    querySet?: string;
    queryId?: string;
}

export const QueryErrors: React.FunctionComponent<QueryErrorsProps> = ({
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
        Cluster: { label: nlsHPCC.Cluster, width: 140 },
        Errors: { label: nlsHPCC.Errors },
        State: { label: nlsHPCC.State, width: 120 },
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
        setQuery(ESPQuery.Get(querySet, queryId));
    }, [setQuery, queryId, querySet]);

    React.useEffect(() => {
        query?.getDetails()
            .then(({ WUQueryDetailsResponse }) => {
                const clusterStates = query?.Clusters?.ClusterQueryState;
                if (clusterStates) {
                    gridStore.setData(clusterStates.map((item, idx) => {
                        return {
                            __hpcc_id: idx,
                            Cluster: item.Cluster,
                            Errors: item.Errors,
                            State: item.State
                        };
                    }));
                    refreshTable();
                }
            })
            .catch(logger.error)
            ;
    }, [gridStore, query, refreshTable]);

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} />}
        main={<DojoGrid store={gridStore} query={gridQuery} sort={gridSort} columns={gridColumns} setGrid={setGrid} setSelection={null} />}
    />;
};