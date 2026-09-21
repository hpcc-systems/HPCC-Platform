import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "./CommandBarV9";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { useQuery } from "../hooks/query";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutoSizeFluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";

interface QueryLibrariesUsedProps {
    querySet?: string;
    queryId?: string;
    sort?: QuerySortItem;
}

const defaultSort = { attribute: "__hpcc_id", descending: false };

export const QueryLibrariesUsed: React.FunctionComponent<QueryLibrariesUsedProps> = ({
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
            Name: { label: nlsHPCC.LibrariesUsed, width: 600 }
        };
    }, []);

    const refreshData = React.useCallback(() => {
        refreshQuery();
    }, [refreshQuery]);

    React.useEffect(() => {
        const librariesUsed = query?.LibrariesUsed?.Item ?? [];
        setData(librariesUsed?.map((item, idx) => {
            return {
                __hpcc_id: idx,
                Name: item
            };
        }));
    }, [query, query?.LibrariesUsed]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
    ], [refreshData]);

    const copyButtons = useCopyButtons(columns, selection, "queryLibraries");

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
