import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link, ScrollablePane, Sticky } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { useFluentGrid } from "../hooks/grid";
import { useWorkunitResources } from "../hooks/workunit";
import { updateParam } from "../util/history";
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
    return `#/workunits/${wuid}/resources/content?url=/WsWorkunits/${url}`;
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

    //  Grid ---
    const { Grid, selection, copyButtons } = useFluentGrid({
        data,
        primaryID: "DisplayPath",
        alphaNumColumns: { Name: true, Value: true },
        sort,
        filename: "resources",
        columns: {
            col1: {
                width: 27,
                selectorType: "checkbox"
            },
            DisplayPath: {
                label: nlsHPCC.Name, sortable: true,
                formatter: React.useCallback(function (url, row) {
                    return <Link href={formatUrl(wuid, row.URL)}>{url}</Link>;
                }, [wuid])
            }
        }
    });

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

    return <ScrollablePane>
        <Sticky>
            <CommandBar items={buttons} farItems={copyButtons} />
        </Sticky>
        {preview && webUrl ?
            <IFrame src={webUrl} /> :
            <Grid />}
    </ScrollablePane>;
};
