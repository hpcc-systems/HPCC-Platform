import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "./CommandBarV9";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { useQuery } from "../hooks/query";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutoSizeFluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";

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

    const [query, , refreshQuery] = useQuery(querySet, queryId);
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
        refreshQuery();
    }, [refreshQuery]);

    React.useEffect(() => {
        const clusterStates = query?.Clusters?.ClusterQueryState ?? [];
        setData(clusterStates?.map((item, idx) => {
            return {
                __hpcc_id: idx,
                Cluster: item.Cluster,
                Errors: item.Errors,
                State: item.State
            };
        }));
    }, [query, query?.Clusters]);

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
        main={<AutoSizeFluentGrid
            data={data}
            primaryID={"__hpcc_id"}
            sort={sort}
            columns={columns}
            setSelection={setSelection}
            setTotal={setTotal}
            refresh={refreshTable}
        ></AutoSizeFluentGrid>}
    />;
};
