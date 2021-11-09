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

const logger = scopedLogger("src-react/components/XrefFoundFiles.tsx");

const defaultUIState = {
    hasSelection: false,
};
interface XrefFoundFilesProps {
    name: string;
}

export const XrefFoundFiles: React.FunctionComponent<XrefFoundFilesProps> = ({
    name
}) => {

    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Grid ---
    const store = useConst(new Observable(new Memory("name")));
    const [Grid, selection, refreshTable, copyButtons] = useGrid({
        store,
        query: {},
        sort: [{ attribute: "modified", "descending": false }],
        filename: "xrefsFoundFiles",
        columns: {
            check: selector({ width: 27 }, "checkbox"),
            name: { width: 180, label: nlsHPCC.Name },
            modified: { width: 80, label: nlsHPCC.Modified },
            parts: { width: 80, label: nlsHPCC.Parts },
            size: { width: 80, label: nlsHPCC.Size }
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
        WsDFUXref.DFUXRefFoundFiles(name)
            .then(rows => {
                if (rows.length) {
                    store.setData(rows.map((item, idx) => {
                        return {
                            name: item.Name,
                            modified: item.Modified,
                            parts: item.Parts,
                            size: item.Size
                        };
                    }));

                    refreshTable();
                }
            })
            .catch(err => logger.error(err))
            ;
    }, [name, refreshTable, store]);

    const [AttachConfirm, setShowAttachConfirm] = useConfirm({
        title: nlsHPCC.Attach,
        message: nlsHPCC.AddTheseFilesToDali + "\n" + selection.map(file => file.name).join("\n"),
        onSubmit: React.useCallback(() => {
            WsDFUXref.DFUXRefArrayAction(selection, nlsHPCC.Attach, name, "Found")
                .then(response => {
                    refreshData();
                })
                .catch(err => logger.error(err))
                ;
        }, [name, refreshData, selection])
    });

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedFiles + "\n" + selection.map(file => file.name),
        onSubmit: React.useCallback(() => {
            WsDFUXref.DFUXRefArrayAction(selection, nlsHPCC.Delete, name, "Found")
                .then(response => {
                    refreshData();
                })
                .catch(err => logger.error(err))
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
            key: "attach", text: nlsHPCC.Attach, disabled: !uiState.hasSelection,
            onClick: () => setShowAttachConfirm(true)
        },
        {
            key: "delete", text: nlsHPCC.Delete, disabled: !uiState.hasSelection,
            onClick: () => setShowDeleteConfirm(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
    ], [refreshData, setShowAttachConfirm, setShowDeleteConfirm, uiState]);

    React.useEffect(() => {
        refreshData();
    }, [refreshData]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <>
                <Grid />
                <AttachConfirm />
                <DeleteConfirm />
            </>
        }
    />;

};