import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { HolyGrail } from "../layouts/HolyGrail";
import * as WsDFUXref from "src/WsDFUXref";
import { useFluentGrid } from "../hooks/grid";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/XrefErrors.tsx");

interface XrefErrorsProps {
    name: string;
}

export const XrefErrors: React.FunctionComponent<XrefErrorsProps> = ({
    name
}) => {

    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const { Grid, copyButtons } = useFluentGrid({
        data,
        primaryID: "name",
        sort: { attribute: "name", descending: false },
        filename: "xrefsErrorsWarnings",
        columns: {
            file: { width: 100, label: nlsHPCC.File },
            text: { width: 50, label: nlsHPCC.Message },
            status: {
                label: nlsHPCC.Status, width: 10, sortable: true,
                renderCell: (object, value, node, options) => {
                    switch (value) {
                        case "Error":
                            node.classList.add("ErrorCell");
                            break;
                        case "Warning":
                            node.classList.add("WarningCell");
                            break;
                        case "Normal":
                            node.classList.add("NormalCell");
                            break;
                    }
                    node.innerText = value;
                }
            }
        }
    });

    const refreshData = React.useCallback(() => {
        WsDFUXref.DFUXRefMessages({ request: { Cluster: name } })
            .then(({ DFUXRefMessagesQueryResponse }) => {
                const { Error = [], Warning = [] } = DFUXRefMessagesQueryResponse?.DFUXRefMessagesQueryResult ?? {};
                const rows = [];
                Warning.map((item, idx) => {
                    rows.push({
                        file: item.File,
                        text: item.Text,
                        status: nlsHPCC.Warning
                    });
                });
                Error.map((item, idx) => {
                    rows.push({
                        file: item.File,
                        text: item.Text,
                        status: nlsHPCC.Error
                    });
                });
                if (rows.length > 0) {
                    setData(rows);
                }
            })
            .catch(err => logger.error(err))
            ;
    }, [name]);

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