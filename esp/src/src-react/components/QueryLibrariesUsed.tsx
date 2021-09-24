import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import * as ESPQuery from "src/ESPQuery";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { useGrid } from "../hooks/grid";
import { HolyGrail } from "../layouts/HolyGrail";

const logger = scopedLogger("src-react/components/QueryLibrariesUsed.tsx");

interface QueryLibrariesUsedProps {
    querySet?: string;
    queryId?: string;
}

export const QueryLibrariesUsed: React.FunctionComponent<QueryLibrariesUsedProps> = ({
    querySet,
    queryId
}) => {

    const [query, setQuery] = React.useState<any>();

    //  Grid ---
    const store = useConst(new Observable(new Memory("__hpcc_id")));
    const [Grid, _selection, refreshTable, copyButtons] = useGrid({
        store,
        sort: [{ attribute: "__hpcc_id" }],
        filename: "queryLibraries",
        columns: {
            Name: { label: nlsHPCC.LibrariesUsed }
        }
    });

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
                const librariesUsed = query?.LibrariesUsed?.Item;
                if (librariesUsed) {
                    store.setData(librariesUsed.map((item, idx) => {
                        return {
                            __hpcc_id: idx,
                            Name: item
                        };
                    }));
                    refreshTable();
                }
            })
            .catch(logger.error)
            ;
    }, [store, query, refreshTable]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={<Grid />}
    />;
};