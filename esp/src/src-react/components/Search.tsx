import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Pivot, PivotItem, ProgressIndicator } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { ESPSearch } from "src/ESPSearch";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { ShortVerticalDivider } from "./Common";
import { DojoGrid, selector } from "./DojoGrid";
import { Workunits } from "./Workunits";
import { Files } from "./Files";
import { Queries } from "./Queries";
import { DFUWorkunits } from "./DFUWorkunits";

const defaultUIState = {
    hasSelection: false,
};

const disabled = { disabled: true, style: { color: "grey" } };

interface SearchProps {
    searchText: string;
}

export const Search: React.FunctionComponent<SearchProps> = ({
    searchText
}) => {

    const progress = useConst({ value: 0 });

    const [grid, setGrid] = React.useState<any>(undefined);
    const [mine, setMine] = React.useState(false);
    const [selection, setSelection] = React.useState([]);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [searchCount, setSearchCount] = React.useState(0);

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

    React.useEffect(() => {
        if (searchText) {
            progress.value = 0;
            setSearchCount(0);
            search.searchAll(searchText);
            refreshTable();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [searchText]);

    //  Command Bar  ---
    const buttons: ICommandBarItemProps[] = [
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
                        window.open(`#/workunits/${selection[i].Wuid}`, "_blank");
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
    ];

    const rightButtons: ICommandBarItemProps[] = [
        {
            key: "copy", text: nlsHPCC.CopyWUIDs, disabled: !uiState.hasSelection || !navigator?.clipboard?.writeText, iconOnly: true, iconProps: { iconName: "Copy" },
            onClick: () => {
                const wuids = selection.map(s => s.Wuid);
                navigator?.clipboard?.writeText(wuids.join("\n"));
            }
        },
        {
            key: "download", text: nlsHPCC.DownloadToCSV, disabled: !uiState.hasSelection, iconOnly: true, iconProps: { iconName: "Download" },
            onClick: () => {
                Utility.downloadToCSV(grid, selection.map(row => ([row.Protected, row.Wuid, row.Owner, row.Jobname, row.Cluster, row.RoxieCluster, row.State, row.TotalClusterTime])), "workunits.csv");
            }
        }
    ];

    //  Grid ---
    const gridColumns = useConst({
        col1: selector({ width: 27, selectorType: "checkbox" }),
        Type: {
            label: nlsHPCC.What, width: 108, sortable: true,
            formatter: function (type, idx) {
                return "<a href='#' onClick='return false;' rowIndex=" + idx + " class='" + "SearchTypeClick'>" + type + "</a>";
            }
        },
        Reason: { label: nlsHPCC.Where, width: 108, sortable: true },
        Summary: {
            label: nlsHPCC.Who, sortable: true,
            formatter: function (summary) {
                return "<a href='#' onClick='return false;' class='dgrid-row-url'>" + summary + "</a>";
            }
        }
    });

    const refreshTable = (clearSelection = false) => {
        grid?.set("query", {});
        if (clearSelection) {
            grid?.clearSelection();
        }
    };

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
        header={<Pivot headersOnly={true} onLinkClick={(item: PivotItem) => setSelectedKey(item.props.itemKey!)}>
            <PivotItem itemKey="all" headerText={nlsHPCC.All} itemCount={search.store.data.length} />
            <PivotItem itemKey="ecl" headerText={nlsHPCC.ECLWorkunit} headerButtonProps={search.eclStore.data.length === 0 ? disabled : undefined} itemCount={search.eclStore.data.length} />
            <PivotItem itemKey="dfu" headerText={nlsHPCC.DFUWorkunit} headerButtonProps={search.dfuStore.data.length === 0 ? disabled : undefined} itemCount={search.dfuStore.data.length} />
            <PivotItem itemKey="file" headerText={nlsHPCC.LogicalFile} headerButtonProps={search.fileStore.data.length === 0 ? disabled : undefined} itemCount={search.fileStore.data.length} />
            <PivotItem itemKey="query" headerText={nlsHPCC.Query} headerButtonProps={search.queryStore.data.length === 0 ? disabled : undefined} itemCount={search.queryStore.data.length} />
        </Pivot>}
        main={selectedKey === "all" ? <HolyGrail
            header={<>
                <CommandBar items={buttons} overflowButtonProps={{}} farItems={rightButtons} />
                <ProgressIndicator progressHidden={searchCount === 0} percentComplete={searchCount === 0 ? 0 : progress.value / searchCount} />
            </>}
            main={<DojoGrid store={search.store} columns={gridColumns} setGrid={setGrid} setSelection={setSelection} />}
        /> : selectedKey === "ecl" ?
                <Workunits store={search.eclStore} /> : selectedKey === "dfu" ?
                    <DFUWorkunits store={search.dfuStore} /> : selectedKey === "file" ?
                        <Files store={search.fileStore} /> : selectedKey === "query" ?
                            <Queries store={search.queryStore} /> :
                            undefined
        }
    />;
};
