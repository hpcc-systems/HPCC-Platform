import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import { HolyGrail } from "../layouts/HolyGrail";
import * as WsDFUXref from "src/WsDFUXref";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import { useConfirm } from "../hooks/confirm";
import { useGrid } from "../hooks/grid";
import { ShortVerticalDivider } from "./Common";
import { selector } from "./DojoGrid";
import nlsHPCC from "src/nlsHPCC";

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

    //  Grid ---
    const store = useConst(new Observable(new Memory("name")));
    const [Grid, selection, refreshTable, copyButtons] = useGrid({
        store,
        query: {},
        sort: [{ attribute: "modified", "descending": false }],
        filename: "xrefsLostFiles",
        columns: {
            check: selector({ width: 27 }, "checkbox"),
            name: { width: 180, label: nlsHPCC.Name },
            modified: { width: 80, label: nlsHPCC.Modified },
            numParts: { width: 80, label: nlsHPCC.TotalParts },
            size: { width: 80, label: nlsHPCC.Size },
            partsLost: { width: 80, label: nlsHPCC.PartsLost },
            primarylost: { width: 80, label: nlsHPCC.PrimaryLost },
            replicatedlost: { width: 80, label: nlsHPCC.ReplicatedLost }
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
        WsDFUXref.DFUXRefLostFiles(name)
            .then(rows => {
                if (rows.length) {
                    store.setData(rows.map((item, idx) => {
                        return {
                            name: item.Name,
                            modified: item.Modified,
                            numParts: item.Numparts,
                            size: item.Size,
                            partsLost: item.Partslost,
                            primarylost: item.Primarylost,
                            replicatedlost: item.Replicatedlost
                        };
                    }));

                    refreshTable();
                }
            })
            .catch(logger.error)
            ;
    }, [store, name, refreshTable]);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedFiles + "\n" + selection.map(file => file.name),
        onSubmit: React.useCallback(() => {
            WsDFUXref.DFUXRefArrayAction(selection, "DeleteLogical", name, "Lost")
                .then(response => {
                    refreshData();
                })
                .catch(logger.error)
                ;
        }, [name, refreshData, selection])
    });

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

    React.useEffect(() => {
        refreshData();
    }, [refreshData]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <>
                <Grid />
                <DeleteConfirm />
            </>
        }
    />;

};