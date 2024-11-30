import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { useWorkunitResources } from "../hooks/workunit";
import { hashHistory, updateParam } from "../util/history";
import { SearchParams } from "../util/hashUrl";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { ShortVerticalDivider } from "./Common";
import { IFrame } from "./IFrame";

const defaultUIState = {
    hasSelection: false
};

interface ResourcesProps {
    wuid: string;
    sort?: QuerySortItem;
    preview?: boolean;
}

const defaultSort = { attribute: "Wuid", descending: true };

function formatUrl(wuid: string, url: string) {
    const searchParams = new SearchParams(hashHistory.location.search);
    searchParams.param("url", `/WsWorkunits/${url}`);
    return `#/workunits/${wuid}/resources/content?${searchParams.serialize()}`;
}

export const Resources: React.FunctionComponent<ResourcesProps> = ({
    wuid,
    sort = defaultSort,
    preview = true
}) => {
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [resources, , , refreshData] = useWorkunitResources(wuid);
    const [data, setData] = React.useState<any[]>([]);
    const [webUrl, setWebUrl] = React.useState("");
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            col1: {
                width: 27,
                selectorType: "checkbox"
            },
            DisplayPath: {
                label: nlsHPCC.Name, sortable: true,
                width: 800,
                formatter: (url, row) => {
                    return <Link href={formatUrl(wuid, row.URL)}>{url}</Link>;
                }
            }
        };
    }, [wuid]);

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
                    window.location.href = formatUrl(wuid, selection[0].URL);
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(formatUrl(wuid, selection[i].URL), "_blank");
                    }
                }
            }
        },
        {
            key: "preview", text: nlsHPCC.Preview, canCheck: true, checked: preview, iconProps: { iconName: "FileHTML" },
            onClick: () => {
                updateParam("preview", !preview);
            }
        },
    ], [refreshData, selection, uiState.hasSelection, wuid, preview]);

    const copyButtons = useCopyButtons(columns, selection, "resources");

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        for (let i = 0; i < selection.length; ++i) {
            state.hasSelection = true;
            break;
        }
        setUIState(state);
    }, [selection]);

    React.useEffect(() => {
        setData(resources.map(row => {
            if (row.endsWith("/index.html")) {
                setWebUrl(`/WsWorkunits/${row}`);
            }
            return {
                URL: row,
                DisplayPath: row.indexOf(`res/${wuid}/`) === 0 ?
                    row.substring(`res/${wuid}/`.length) :
                    row
            };
        }));
    }, [resources, wuid]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            preview && webUrl ?
                <IFrame src={webUrl} /> :
                <FluentGrid
                    data={data}
                    primaryID={"DisplayPath"}
                    alphaNumColumns={{ Value: true }}
                    sort={sort}
                    columns={columns}
                    setSelection={setSelection}
                    setTotal={setTotal}
                    refresh={refreshTable}
                ></FluentGrid>
        }
    />;
};
