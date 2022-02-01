import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import * as ESPQuery from "src/ESPQuery";
import nlsHPCC from "src/nlsHPCC";
import { useFluentGrid } from "../hooks/grid";
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

    const query = React.useMemo(() => {
        return ESPQuery.Get(querySet, queryId);
    }, [querySet, queryId]);
    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const [Grid, _selection, copyButtons] = useFluentGrid({
        data,
        primaryID: "__hpcc_id",
        sort: [{ attribute: "__hpcc_id" }],
        filename: "queryLibraries",
        columns: {
            Name: { label: nlsHPCC.LibrariesUsed }
        }
    });

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

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={<Grid />}
    />;
};
