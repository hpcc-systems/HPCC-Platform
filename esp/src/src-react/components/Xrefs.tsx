import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { HolyGrail } from "../layouts/HolyGrail";
import nlsHPCC from "src/nlsHPCC";
import * as WsDFUXref from "src/WsDFUXref";
import { useConfirm } from "../hooks/confirm";
import { useBuildInfo } from "../hooks/platform";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { ShortVerticalDivider } from "./Common";
import { pushUrl } from "../util/history";

const logger = scopedLogger("src-react/components/Xrefs.tsx");

const defaultUIState = {
    hasSelection: false,
};

interface XrefsProps {
}

export const Xrefs: React.FunctionComponent<XrefsProps> = ({
}) => {

    const [, { opsCategory }] = useBuildInfo();

    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Grid ---

    const columns = React.useMemo((): FluentColumns => {
        return {
            check: { width: 27, selectorType: "checkbox" },
            name: {
                width: 180,
                label: nlsHPCC.Name,
                formatter: (_name, idx) => {
                    return <Link href={`#/xref/${_name}`}>{_name}</Link>;
                }
            },
            modified: { width: 180, label: nlsHPCC.LastRun },
            status: { width: 180, label: nlsHPCC.LastMessage }
        };
    }, []);

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
                    if (Array.isArray(xrefNodes)) {
                        setData(xrefNodes.map((item, idx) => {
                            return {
                                name: item.Name,
                                modified: item.Modified,
                                status: item.Status
                            };
                        }));
                    } else {
                        setData([{
                            name: xrefNodes.Name,
                            modified: xrefNodes.Modified,
                            status: xrefNodes.Status
                        }]);
                    }
                }
            })
            .catch(err => logger.error(err))
            ;
    }, []);

    React.useEffect(() => {
        refreshData();
    }, [refreshData]);

    const [CancelConfirm, setShowCancelConfirm] = useConfirm({
        title: nlsHPCC.CancelAll,
        message: nlsHPCC.CancelAllMessage,
        onSubmit: React.useCallback(() => {
            WsDFUXref.DFUXRefBuildCancel({
                request: {}
            })
                .catch(err => logger.error(err))
                ;
        }, [])
    });

    const [GenerateConfirm, setShowGenerateConfirm] = useConfirm({
        title: nlsHPCC.Generate,
        message: nlsHPCC.RunningServerStrain,
        onSubmit: React.useCallback(() => {
            const requests = [];
            for (let i = selection.length - 1; i >= 0; --i) {
                requests.push(
                    WsDFUXref.DFUXRefBuild({
                        request: {
                            Cluster: selection[i].name
                        }
                    })
                );
            }
            Promise.all(requests)
                .then(() => {
                    refreshData();
                })
                .catch(err => logger.error(err))
                ;
        }, [refreshData, selection])
    });

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
                    pushUrl(`/${opsCategory}/security/users/${selection[0].username}`);
                } else {
                    selection.forEach(user => {
                        window.open(`#/${opsCategory}/security/users/${user.username}`, "_blank");
                    });
                }
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "cancelAll", text: nlsHPCC.CancelAll,
            onClick: () => setShowCancelConfirm(true)
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "generate", text: nlsHPCC.Generate,
            onClick: () => setShowGenerateConfirm(true)
        }
    ], [opsCategory, refreshData, selection, setShowCancelConfirm, setShowGenerateConfirm, uiState]);

    const copyButtons = useCopyButtons(columns, selection, "xrefs");

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <>
                <FluentGrid
                    data={data}
                    primaryID={"name"}
                    sort={{ attribute: "modified", descending: false }}
                    columns={columns}
                    setSelection={setSelection}
                    setTotal={setTotal}
                    refresh={refreshTable}
                ></FluentGrid>
                <CancelConfirm />
                <GenerateConfirm />
            </>
        }
    />;
};