import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { HolyGrail } from "../layouts/HolyGrail";
import * as WsDFUXref from "src/WsDFUXref";
import { useConfirm } from "../hooks/confirm";
import { useFluentGrid } from "../hooks/grid";
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
    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const [Grid, selection, copyButtons] = useFluentGrid({
        data,
        primaryID: "name",
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
                    setData(rows.map((item, idx) => {
                        return {
                            name: item.Name,
                            modified: item.Modified,
                            partsFound: item.PartsFound,
                            totalParts: item.TotalParts,
                            size: item.Size
                        };
                    }));
                }
            })
            .catch(err => logger.error(err))
            ;
    }, [name]);

    React.useEffect(() => {
        refreshData();
    }, [refreshData]);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedFiles,
        items: selection.map(file => file.name),
        onSubmit: React.useCallback(() => {
            WsDFUXref.DFUXRefArrayAction(selection, nlsHPCC.Delete, name, "Orphan")
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
            key: "delete", text: nlsHPCC.Delete, disabled: !uiState.hasSelection,
            onClick: () => setShowDeleteConfirm(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
    ], [refreshData, setShowDeleteConfirm, uiState]);

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