import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Pivot, PivotItem, ProgressIndicator } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { ESPSearch } from "src/ESPSearch";
import nlsHPCC from "src/nlsHPCC";
import { useGrid } from "../hooks/grid";
import { HolyGrail } from "../layouts/HolyGrail";
import { ShortVerticalDivider } from "./Common";
import { selector } from "./DojoGrid";
import { Workunits } from "./Workunits";
import { Files } from "./Files";
import { Queries } from "./Queries";
import { DFUWorkunits } from "./DFUWorkunits";

const defaultUIState = {
    hasSelection: false,
};

const disabled = { disabled: true, style: { color: "grey" } };

const searchResultUrl = (result) => {
    let url = window.location.hash;
    switch (result.Type) {
        case "ECL Workunit":
            url = "#/workunits/" + result._wuid;
            break;
        case "DFU Workunit":
            url = "#/dfuworkunits/" + result._wuid;
            break;
        case "Logical File":
            url = "#/files/" + result._nodeGroup + "/" + result._name;
            break;
        case "Query":
            url = "#/queries/" + result._querySetId + "/" + result._id;
            break;
    }
    return url;
};

interface SearchProps {
    searchText: string;
}

export const Search: React.FunctionComponent<SearchProps> = ({
    searchText
}) => {

    const progress = useConst({ value: 0 });

    const [mine, setMine] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [searchCount, setSearchCount] = React.useState(0);

    //  Search
    const search = useConst(new ESPSearch(
        searchCount => {
            setSearchCount(searchCount);
        },
        () => {
            progress.value++;
            refreshTable();
        }, () => {
            setSearchCount(0);
        }));

    //  Grid ---
    const [Grid, selection, refreshTable, copyButtons] = useGrid({
        store: search.store,
        filename: "search",
        columns: {
            col1: selector({ width: 27, selectorType: "checkbox" }),
            Type: {
                label: nlsHPCC.What, width: 108, sortable: true,
                formatter: function (type, row) {
                    return "<a href='" + searchResultUrl(row) + "'>" + type + "</a>";
                }
            },
            Reason: { label: nlsHPCC.Where, width: 108, sortable: true },
            Summary: {
                label: nlsHPCC.Who, sortable: true,
                formatter: function (summary, row) {
                    return "<a href='" + searchResultUrl(row) + "'>" + summary + "</a>";
                }
            }
        }
    });

    React.useEffect(() => {
        if (searchText) {
            progress.value = 0;
            setSearchCount(0);
            search.searchAll(searchText);
            refreshTable();
        }
    }, [progress, refreshTable, search, searchText]);

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
                    window.location.href = `#/workunits/${selection[0].Wuid}`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(searchResultUrl(selection[i]), "_blank");
                    }
                }
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "mine", text: nlsHPCC.Mine, disabled: true, iconProps: { iconName: "Contact" }, canCheck: true, checked: mine,
            onClick: () => {
                setMine(!mine);
            }
        },
    ], [mine, refreshTable, selection, uiState.hasSelection]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        for (let i = 0; i < selection.length; ++i) {
            state.hasSelection = true;
        }
        setUIState(state);
    }, [selection]);

    const [selectedKey, setSelectedKey] = React.useState("all");

    return <HolyGrail
        header={<Pivot headersOnly={true} onLinkClick={(item: PivotItem) => setSelectedKey(item.props.itemKey!)
        }>
            <PivotItem itemKey="all" headerText={nlsHPCC.All} itemCount={search.store.data.length} />
            <PivotItem itemKey="ecl" headerText={nlsHPCC.ECLWorkunit} headerButtonProps={search.eclStore.data.length === 0 ? disabled : undefined} itemCount={search.eclStore.data.length} />
            <PivotItem itemKey="dfu" headerText={nlsHPCC.DFUWorkunit} headerButtonProps={search.dfuStore.data.length === 0 ? disabled : undefined} itemCount={search.dfuStore.data.length} />
            <PivotItem itemKey="file" headerText={nlsHPCC.LogicalFile} headerButtonProps={search.fileStore.data.length === 0 ? disabled : undefined} itemCount={search.fileStore.data.length} />
            <PivotItem itemKey="query" headerText={nlsHPCC.Query} headerButtonProps={search.queryStore.data.length === 0 ? disabled : undefined} itemCount={search.queryStore.data.length} />
        </Pivot >}
        main={selectedKey === "all" ? <HolyGrail
            header={<>
                <CommandBar items={buttons} farItems={copyButtons} />
                <ProgressIndicator progressHidden={searchCount === 0} percentComplete={searchCount === 0 ? 0 : progress.value / searchCount} />
            </>}
            main={<Grid />}
        /> : selectedKey === "ecl" ?
            <Workunits store={search.eclStore} /> : selectedKey === "dfu" ?
                <DFUWorkunits store={search.dfuStore} /> : selectedKey === "file" ?
                    <Files store={search.fileStore} /> : selectedKey === "query" ?
                        <Queries store={search.queryStore} /> :
                        undefined
        }
    />;
};
