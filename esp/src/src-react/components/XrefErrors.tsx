import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import { HolyGrail } from "../layouts/HolyGrail";
import * as WsDFUXref from "src/WsDFUXref";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import { useGrid } from "../hooks/grid";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/XrefErrors.tsx");

interface XrefErrorsProps {
    name: string;
}

export const XrefErrors: React.FunctionComponent<XrefErrorsProps> = ({
    name
}) => {

    //  Grid ---
    const store = useConst(new Observable(new Memory("name")));
    const [Grid, _selection, refreshTable, copyButtons] = useGrid({
        store,
        query: {},
        sort: [{ attribute: "name", "descending": false }],
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
                const { Error = [], Warning = [] } = DFUXRefMessagesQueryResponse?.DFUXRefMessagesQueryResult;
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
                    store.setData(rows);
                    refreshTable();
                }
            })
            .catch(logger.error)
            ;
    }, [name, refreshTable, store]);

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

    React.useEffect(() => {
    }, [store, name, refreshTable]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={<Grid />}
    />;

};