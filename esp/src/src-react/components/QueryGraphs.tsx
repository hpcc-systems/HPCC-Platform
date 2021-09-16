import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import * as Observable from "dojo/store/Observable";
import * as ESPQuery from "src/ESPQuery";
import { Memory } from "src/Memory";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { DojoGrid, selector } from "./DojoGrid";

const logger = scopedLogger("src-react/components/QueryGraphs.tsx");

function getStateImageName(row) {
    if (row.Complete) {
        return "workunit_completed.png";
    } else if (row.Running) {
        return "workunit_running.png";
    } else if (row.Failed) {
        return "workunit_failed.png";
    }
    return "workunit.png";
}

interface QueryGraphsProps {
    querySet: string;
    queryId: string;
}

export const QueryGraphs: React.FunctionComponent<QueryGraphsProps> = ({
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
        col1: selector({ width: 27, selectorType: "checkbox" }),
        Name: {
            label: nlsHPCC.Name,
            formatter: function (Name, row) {
                const url = `#/queries/${querySet}/${queryId}/graphs/${row.Wuid}/${Name}`;
                return Utility.getImageHTML(getStateImageName(row)) + `&nbsp;<a href='${url}' class='dgrid-row-url'>${Name}</a>`;
            }
        },
        Type: { label: nlsHPCC.Type, width: 72 },
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
                const graphs = query?.WUGraphs?.ECLGraph;
                if (graphs) {
                    gridStore.setData(graphs.map((item, idx) => {
                        return {
                            __hpcc_id: idx,
                            Name: item.Name,
                            Label: "",
                            Wuid: query.Wuid,
                            Completed: "",
                            Time: 0,
                            Type: item.Type
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