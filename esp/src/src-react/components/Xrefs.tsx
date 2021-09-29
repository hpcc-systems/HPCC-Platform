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
import { selector } from "./DojoGrid";
import { pushUrl } from "../util/history";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/Xrefs.tsx");

const defaultUIState = {
    hasSelection: false,
};

interface XrefsProps {
}

export const Xrefs: React.FunctionComponent<XrefsProps> = ({
}) => {

    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Grid ---
    const store = useConst(new Observable(new Memory("name")));
    const [Grid, selection, refreshTable, copyButtons] = useGrid({
        store,
        query: {},
        sort: [{ attribute: "modified", "descending": false }],
        filename: "xrefs",
        columns: {
            check: selector({ width: 27 }, "checkbox"),
            name: {
                width: 180,
                label: nlsHPCC.Name,
                formatter: function (_name, idx) {
                    return `<a href="#/xref/${_name}">${_name}</a>`;
                }
            },
            modified: { width: 180, label: nlsHPCC.LastRun },
            status: { width: 180, label: nlsHPCC.LastMessage }
        }
    });

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        if (selection.length > 0) {
            state.hasSelection = true;
        }

        setUIState(state);
    }, [selection]);

    const refreshData = React.useCallback(() => {
        WsDFUXref.WUGetXref({
            request: {}
        })
            .then(({ DFUXRefListResponse }) => {
                const xrefNodes = DFUXRefListResponse?.DFUXRefListResult?.XRefNode;
                if (xrefNodes) {
                    store.setData(xrefNodes.map((item, idx) => {
                        return {
                            name: item.Name,
                            modified: item.Modified,
                            status: item.Status
                        };
                    }));

                    refreshTable();
                }
            })
            .catch(logger.error)
            ;
    }, [refreshTable, store]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.hasSelection,
            onClick: () => {
                if (selection.length === 1) {
                    pushUrl(`/security/users/${selection[0].username}`);
                } else {
                    selection.forEach(user => {
                        window.open(`#/security/users/${user.username}`, "_blank");
                    });
                }
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "cancelAll", text: nlsHPCC.CancelAll,
            onClick: () => {
                if (confirm(nlsHPCC.CancelAllMessage)) {
                    WsDFUXref.DFUXRefBuildCancel({
                        request: {}
                    })
                        .catch(logger.error)
                        ;
                }
            }
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "generate", text: nlsHPCC.Generate,
            onClick: () => {
                if (confirm(nlsHPCC.RunningServerStrain)) {
                    const requests = [];
                    for (let i = selection.length - 1; i >= 0; --i) {
                        requests.push(
                            WsDFUXref.DFUXRefBuild({
                                request: {
                                    Cluster: selection[i].name
                                }
                            })
                        );

                        Promise.all(requests)
                            .then(() => {
                                refreshData();
                            })
                            .catch(logger.error)
                            ;
                    }
                }
            },
        }
    ], [refreshData, selection, uiState]);

    React.useEffect(() => {
        refreshData();
    }, [refreshData]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={<Grid />}
    />;

};