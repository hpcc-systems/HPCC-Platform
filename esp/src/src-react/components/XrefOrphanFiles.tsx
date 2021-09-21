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
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/XrefOrphanFiles.tsx");

const defaultUIState = {
    hasSelection: false,
};
interface XrefOrphanFilesProps {
    name: string;
}

export const XrefOrphanFiles: React.FunctionComponent<XrefOrphanFilesProps> = ({
    name
}) => {

    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Grid ---
    const store = useConst(new Observable(new Memory("name")));
    const [Grid, selection, refreshTable, copyButtons] = useGrid({
        store,
        query: {},
        sort: [{ attribute: "modified", "descending": false }],
        filename: "xrefsOrphanFiles",
        columns: {
            check: selector({ width: 27 }, "checkbox"),
            name: { width: 180, label: nlsHPCC.Name },
            modified: { width: 80, label: nlsHPCC.Modified },
            partsFound: { width: 80, label: nlsHPCC.PartsFound },
            totalParts: { width: 80, label: nlsHPCC.TotalParts },
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
        WsDFUXref.DFUXRefOrphanFiles(name)
            .then(rows => {
                if (rows.length) {
                    store.setData(rows.map((item, idx) => {
                        return {
                            name: item.Name,
                            modified: item.Modified,
                            partsFound: item.PartsFound,
                            totalParts: item.TotalParts,
                            size: item.Size
                        };
                    }));

                    refreshTable();
                }
            })
            .catch(logger.error)
            ;
    }, [name, refreshTable, store]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "delete", text: nlsHPCC.Delete, disabled: !uiState.hasSelection,
            onClick: () => {
                const list = selection.map(file => file.name);
                if (confirm(nlsHPCC.DeleteSelectedFiles + "\n" + list)) {
                    WsDFUXref.DFUXRefArrayAction(selection, nlsHPCC.Delete, name, "Orphan")
                        .then(response => {
                            refreshData();
                        })
                        .catch(logger.error)
                        ;
                }
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
    ], [name, refreshData, selection, uiState]);

    React.useEffect(() => {
        refreshData();
    }, [refreshData]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={<Grid />}
    />;

};