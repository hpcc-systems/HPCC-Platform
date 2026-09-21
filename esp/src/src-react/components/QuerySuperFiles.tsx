import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "./CommandBarV9";
import { Link } from "@fluentui/react-components";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { useQuery } from "../hooks/query";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushUrl } from "../util/history";
import { AutoSizeFluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";

const defaultUIState = {
    hasSelection: false
};

interface QuerySuperFilesProps {
    querySet?: string;
    queryId?: string;
    sort?: QuerySortItem;
}

const defaultSort = { attribute: "__hpcc_id", descending: false };

export const QuerySuperFiles: React.FunctionComponent<QuerySuperFilesProps> = ({
    querySet,
    queryId,
    sort = defaultSort
}) => {

    const [query, , refreshQuery] = useQuery(querySet, queryId);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            col1: { selectorType: "checkbox", width: 25 },
            File: {
                label: nlsHPCC.File, width: 600,
                formatter: (item, row) => {
                    return <Link href={`#/files/${item}`}>{item}</Link>;
                }
            }
        };
    }, []);

    const refreshData = React.useCallback(() => {
        refreshQuery();
    }, [refreshQuery]);

    React.useEffect(() => {
        const superFiles = query?.SuperFiles?.SuperFile ?? [];
        setData(superFiles?.map((item, idx) => {
            return {
                __hpcc_id: idx,
                File: item.Name
            };
        }));
    }, [query, query?.SuperFiles]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider },
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

    const copyButtons = useCopyButtons(columns, selection, "querySuperFiles");

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
        main={<AutoSizeFluentGrid
            data={data}
            primaryID={"__hpcc_id"}
            sort={sort}
            columns={columns}
            setSelection={setSelection}
            setTotal={setTotal}
            refresh={refreshTable}
        ></AutoSizeFluentGrid>}
    />;
};
