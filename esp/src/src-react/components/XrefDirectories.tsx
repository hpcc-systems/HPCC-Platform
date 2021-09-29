import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import { HolyGrail } from "../layouts/HolyGrail";
import * as WsDFUXref from "src/WsDFUXref";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import { useGrid } from "../hooks/grid";
import { ShortVerticalDivider } from "./Common";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/XrefDirectories.tsx");

interface XrefDirectoriesProps {
    name: string;
}

export const XrefDirectories: React.FunctionComponent<XrefDirectoriesProps> = ({
    name
}) => {

    //  Grid ---
    const store = useConst(new Observable(new Memory("name")));
    const [Grid, _selection, refreshTable, copyButtons] = useGrid({
        store,
        query: {},
        sort: [{ attribute: "name", "descending": false }],
        filename: "xrefsDirectories",
        columns: {
            name: { width: 100, label: nlsHPCC.Name },
            num: { width: 30, label: nlsHPCC.Files },
            size: { width: 30, label: nlsHPCC.TotalSize },
            maxIP: { width: 30, label: nlsHPCC.MaxNode },
            maxSize: { width: 30, label: nlsHPCC.MaxSize },
            minIP: { width: 30, label: nlsHPCC.MinNode },
            minSize: { width: 30, label: nlsHPCC.MinSize },
            positiveSkew: {
                width: 30,
                label: nlsHPCC.SkewPositive,
                renderCell: (object, value, node, options) => {
                    if (value === undefined) {
                        return "";
                    }
                    node.innerText = value;
                }
            },
            negativeSkew: {
                width: 30,
                label: nlsHPCC.SkewNegative,
                renderCell: (object, value, node, options) => {
                    if (value === undefined) {
                        return "";
                    }
                    node.innerText = value;
                }
            }
        }
    });

    const refreshData = React.useCallback(() => {
        WsDFUXref.DFUXRefDirectories({ request: { Cluster: name } })
            .then(({ DFUXRefDirectoriesQueryResponse }) => {
                const directories = DFUXRefDirectoriesQueryResponse?.DFUXRefDirectoriesQueryResult?.Directory;
                if (directories) {
                    store.setData(directories.map((item, idx) => {
                        return {
                            name: item.Name,
                            num: item.Num,
                            size: item.Size,
                            maxIP: item.MaxIP,
                            maxSize: item.MaxSize,
                            minIP: item.MinIP,
                            minSize: item.MinSize,
                            positiveSkew: item.PositiveSkew,
                            negativeSkew: item.NegativeSkew,
                        };
                    }));

                    refreshTable();
                }
            })
            .catch(logger.error)
            ;
    }, [store, name, refreshTable]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "delete", text: nlsHPCC.DeleteEmptyDirectories,
            onClick: () => {
                if (confirm(nlsHPCC.DeleteDirectories)) {
                    WsDFUXref.DFUXRefCleanDirectories({ request: { Cluster: name } })
                        .then(response => {
                            refreshData();
                        })
                        .catch(logger.error)
                        ;
                }
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
    ], [name, refreshData]);

    React.useEffect(() => {
        refreshData();
    }, [refreshData]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={<Grid />}
    />;

};