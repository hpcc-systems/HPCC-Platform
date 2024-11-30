import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import * as ESPQuery from "src/ESPQuery";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";

const logger = scopedLogger("src-react/components/QueryLibrariesUsed.tsx");

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
            Name: { label: nlsHPCC.LibrariesUsed }
        };
    }, []);

    const refreshData = React.useCallback(() => {
        query?.getDetails()
            .then(({ WUQueryDetailsResponse }) => {
                const librariesUsed = query?.LibrariesUsed?.Item;
                if (librariesUsed) {
                    setData(librariesUsed.map((item, idx) => {
                        return {
                            __hpcc_id: idx,
                            Name: item
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

    const copyButtons = useCopyButtons(columns, selection, "queryLibraries");

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
