import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { HolyGrail } from "../layouts/HolyGrail";
import nlsHPCC from "src/nlsHPCC";
import * as WsDFUXref from "src/WsDFUXref";
import { useConfirm } from "../hooks/confirm";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { ShortVerticalDivider } from "./Common";

const logger = scopedLogger("src-react/components/XrefLostFiles.tsx");

const defaultUIState = {
    hasSelection: false,
};
interface XrefLostFilesProps {
    name: string;
}

export const XrefLostFiles: React.FunctionComponent<XrefLostFilesProps> = ({
    name
}) => {

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
            Name: { width: 360, label: nlsHPCC.Name },
            modified: { width: 80, label: nlsHPCC.Modified },
            numParts: { width: 80, label: nlsHPCC.TotalParts },
            size: { width: 80, label: nlsHPCC.Size },
            partsLost: { width: 80, label: nlsHPCC.PartsLost },
            primarylost: { width: 80, label: nlsHPCC.PrimaryLost },
            replicatedlost: { width: 80, label: nlsHPCC.ReplicatedLost }
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
        WsDFUXref.DFUXRefLostFiles(name)
            .then(rows => {
                if (rows.length) {
                    setData(rows.map((item, idx) => {
                        return {
                            Name: item.Name,
                            modified: item.Modified,
                            numParts: item.Numparts,
                            size: item.Size,
                            partsLost: item.Partslost,
                            primarylost: item.Primarylost,
                            replicatedlost: item.Replicatedlost
                        };
                    }));
                }
            })
            .catch(err => logger.error(err))
            ;
    }, [name]);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedFiles,
        items: selection.map(file => file.Name),
        onSubmit: React.useCallback(() => {
            WsDFUXref.DFUXRefArrayAction(selection, "DeleteLogical", name, "Lost")
                .then(response => {
                    refreshData();
                })
                .catch(err => logger.error(err))
                ;
        }, [name, refreshData, selection])
    });

    React.useEffect(() => {
        refreshData();
    }, [refreshData]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "delete", text: nlsHPCC.Delete, disabled: !uiState.hasSelection,
            onClick: () => setShowDeleteConfirm(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
    ], [refreshData, setShowDeleteConfirm, uiState]);

    const copyButtons = useCopyButtons(columns, selection, "xrefsLostFiles");

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <>
                <FluentGrid
                    data={data}
                    primaryID={"Name"}
                    sort={{ attribute: "modified", descending: false }}
                    columns={columns}
                    setSelection={setSelection}
                    setTotal={setTotal}
                    refresh={refreshTable}
                ></FluentGrid>
                <DeleteConfirm />
            </>
        }
    />;

};