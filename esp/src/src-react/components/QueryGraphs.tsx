import * as React from "react";
import { CommandBar, ICommandBarItemProps, Image, Link } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import * as ESPQuery from "src/ESPQuery";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useFluentGrid } from "../hooks/grid";
import { HolyGrail } from "../layouts/HolyGrail";
import { selector } from "./DojoGrid";

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

    const query = React.useMemo(() => {
        return ESPQuery.Get(querySet, queryId);
    }, [querySet, queryId]);
    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const [Grid, _selection, copyButtons] = useFluentGrid({
        data,
        primaryID: "__hpcc_id",
        sort: [{ attribute: "__hpcc_id" }],
        filename: "queryGraphs",
        columns: {
            col1: selector({ width: 27, selectorType: "checkbox" }),
            Name: {
                label: nlsHPCC.Name,
                formatter: function (Name, row) {
                    return <>
                        <Image src={Utility.getImageURL(getStateImageName(row))} />
                        &nbsp;
                        <Link href={`#/workunits/${row.Wuid}/metrics/${Name}`}>{Name}</Link>
                    </>;
                }
            },
            Type: { label: nlsHPCC.Type, width: 72 },
        }
    });

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

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={<Grid />}
    />;
};