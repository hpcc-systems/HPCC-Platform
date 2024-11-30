import * as React from "react";
import { CommandBar, ICommandBarItemProps, Image, Link } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import * as ESPQuery from "src/ESPQuery";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";

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
    sort?: QuerySortItem;
}

const defaultSort = { attribute: "__hpcc_id", descending: false };

export const QueryGraphs: React.FunctionComponent<QueryGraphsProps> = ({
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
            col1: { width: 27, selectorType: "checkbox" },
            Name: {
                label: nlsHPCC.Name,
                formatter: (Name, row) => {
                    return <>
                        <Image src={Utility.getImageURL(getStateImageName(row))} />
                        &nbsp;
                        <Link href={`#/workunits/${row.Wuid}/metrics/${Name}`}>{Name}</Link>
                    </>;
                }
            },
            Type: { label: nlsHPCC.Type, width: 72 },
        };
    }, []);

    const refreshData = React.useCallback(() => {
        query?.getDetails()
            .then(({ WUQueryDetailsResponse }) => {
                const graphs = query?.WUGraphs?.ECLGraph;
                if (graphs) {
                    setData(graphs.map((item, idx) => {
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

    const copyButtons = useCopyButtons(columns, selection, "queryGraphs");

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