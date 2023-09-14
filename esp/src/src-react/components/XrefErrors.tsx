import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import * as WsDFUXref from "src/WsDFUXref";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";

const logger = scopedLogger("src-react/components/XrefErrors.tsx");

interface XrefErrorsProps {
    name: string;
}

export const XrefErrors: React.FunctionComponent<XrefErrorsProps> = ({
    name
}) => {

    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Grid ---

    const columns = React.useMemo((): FluentColumns => {
        return {
            file: { width: 100, label: nlsHPCC.File },
            text: { width: 50, label: nlsHPCC.Message },
            status: {
                label: nlsHPCC.Status, width: 10, sortable: true,
                className: (value, row) => {
                    switch (value) {
                        case "Error":
                            return "ErrorCell";
                        case "Warning":
                            return "WarningCell";
                        case "Normal":
                            return "NormalCell";
                    }
                    return "";
                }
            }
        };
    }, []);

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

    const copyButtons = useCopyButtons(columns, selection, "xrefsErrorsWarnings");

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={<FluentGrid
            data={data}
            primaryID={"name"}
            sort={{ attribute: "name", descending: false }}
            columns={columns}
            setSelection={setSelection}
            setTotal={setTotal}
            refresh={refreshTable}
        ></FluentGrid>}
    />;

};