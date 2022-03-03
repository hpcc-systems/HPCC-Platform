import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import * as ESPQuery from "src/ESPQuery";
import nlsHPCC from "src/nlsHPCC";
import { useFluentGrid } from "../hooks/grid";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushUrl } from "../util/history";
import { ShortVerticalDivider } from "./Common";
import { selector } from "./DojoGrid";

const logger = scopedLogger("src-react/components/QuerySuperFiles.tsx");

const defaultUIState = {
    hasSelection: false
};

interface QuerySuperFilesProps {
    querySet?: string;
    queryId?: string;
}

export const QuerySuperFiles: React.FunctionComponent<QuerySuperFilesProps> = ({
    querySet,
    queryId
}) => {

    const query = React.useMemo(() => {
        return ESPQuery.Get(querySet, queryId);
    }, [querySet, queryId]);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const { Grid, selection, copyButtons } = useFluentGrid({
        data,
        primaryID: "__hpcc_id",
        sort: { attribute: "__hpcc_id", descending: false },
        filename: "querySuperFiles",
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

    const refreshData = React.useCallback(() => {
        query?.getDetails()
            .then(({ WUQueryDetailsResponse }) => {
                const superFiles = query?.SuperFiles?.SuperFile;
                if (superFiles) {
                    setData(superFiles.map((item, idx) => {
                        return {
                            __hpcc_id: idx,
                            File: item.Name
                        };
                    }));
                }
            })
            .catch(err => logger.error(err));
    }, [query]);

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
    ], [querySet, refreshData, selection, uiState.hasSelection]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        if (selection.length) {
            state.hasSelection = true;
        }

        setUIState(state);
    }, [selection]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={<Grid />}
    />;
};