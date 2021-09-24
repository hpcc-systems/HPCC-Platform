import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import * as ESPQuery from "src/ESPQuery";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { useGrid } from "../hooks/grid";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushUrl } from "../util/history";
import { selector } from "./DojoGrid";
import { ShortVerticalDivider } from "./Common";

const logger = scopedLogger("../components/QueryLogicalFiles.tsx");

const defaultUIState = {
    hasSelection: false
};

interface QueryLogicalFilesProps {
    querySet?: string;
    queryId?: string;
}

export const QueryLogicalFiles: React.FunctionComponent<QueryLogicalFilesProps> = ({
    querySet,
    queryId
}) => {

    const [query, setQuery] = React.useState<any>();
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Grid ---
    const store = useConst(new Observable(new Memory("__hpcc_id")));
    const [Grid, selection, refreshTable, copyButtons] = useGrid({
        store,
        sort: [{ attribute: "__hpcc_id" }],
        filename: "queryLogicalFiles",
        columns: {
            col1: selector({ selectorType: "checkbox", width: 25 }),
            File: {
                label: nlsHPCC.File,
                formatter: function (item, row) {
                    return `<a href="#/files/${querySet}/${item}">${item}</a>`;
                }
            },
        }
    });

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.hasSelection, iconProps: { iconName: "WindowEdit" },
            onClick: () => {
                if (selection.length === 1) {
                    pushUrl(`/files/${querySet}/${selection[0].File}`);
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/files/${selection[i].QuerySetId}/${selection[i].Id}`, "_blank");
                    }
                }
            }
        },
    ], [querySet, refreshTable, selection, uiState.hasSelection]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        for (let i = 0; i < selection.length; ++i) {
            state.hasSelection = true;
        }

        setUIState(state);
    }, [selection]);

    React.useEffect(() => {
        setQuery(ESPQuery.Get(querySet, queryId));
    }, [setQuery, queryId, querySet]);

    React.useEffect(() => {
        query?.getDetails()
            .then(({ WUQueryDetailsResponse }) => {
                const logicalFiles = query?.LogicalFiles?.Item;
                if (logicalFiles) {
                    store.setData(logicalFiles.map((item, idx) => {
                        return {
                            __hpcc_id: idx,
                            File: item
                        };
                    }));
                    refreshTable();
                }
            })
            .catch(logger.error)
            ;
    }, [store, query, refreshTable]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={<Grid />}
    />;
};
