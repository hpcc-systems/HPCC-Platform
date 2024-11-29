import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import * as ESPQuery from "src/ESPQuery";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";

const logger = scopedLogger("../components/QueryErrors.tsx");

interface QueryErrorsProps {
    querySet?: string;
    queryId?: string;
    sort?: QuerySortItem;
}

const defaultSort = { attribute: "__hpcc_id", descending: false };

export const QueryErrors: React.FunctionComponent<QueryErrorsProps> = ({
    querySet,
    queryId,
    sort = defaultSort
}) => {

    const query = React.useMemo(() => {
        return ESPQuery.Get(querySet, queryId);
    }, [querySet, queryId]);
    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            Cluster: { label: nlsHPCC.Cluster, width: 140 },
            Errors: { label: nlsHPCC.Errors },
            State: { label: nlsHPCC.State, width: 120 },
        };
    }, []);

    const refreshData = React.useCallback(() => {
        query?.getDetails()
            .then(({ WUQueryDetailsResponse }) => {
                const clusterStates = query?.Clusters?.ClusterQueryState;
                if (clusterStates) {
                    setData(clusterStates.map((item, idx) => {
                        return {
                            __hpcc_id: idx,
                            Cluster: item.Cluster,
                            Errors: item.Errors,
                            State: item.State
                        };
                    }));
                }
            })
            .catch(err => logger.error(err))
            ;
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

    const copyButtons = useCopyButtons(columns, selection, "queryErrors");

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={<FluentGrid
            data={data}
            primaryID={"__hpcc_id"}
            sort={sort}
            columns={columns}
            setSelection={setSelection}
            setTotal={setTotal}
            refresh={refreshTable}
        ></FluentGrid>}
    />;
};
