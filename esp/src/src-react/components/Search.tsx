import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link, Pivot, PivotItem, ProgressIndicator } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { ESPSearch } from "src/ESPSearch";
import nlsHPCC from "src/nlsHPCC";
import { useFluentGrid } from "../hooks/grid";
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

    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [searchCount, setSearchCount] = React.useState(0);

    const [data, setData] = React.useState<any[]>([]);

    //  Search
    const search = useConst(new ESPSearch(() => { progress.value++; }));

    //  Grid ---
    const { Grid, selection, copyButtons } = useFluentGrid({
        data,
        primaryID: "__hpcc_id",
        sort: { attribute: "__hpcc_id", descending: false },
        filename: "search",
        columns: {
            col1: selector({ width: 27, selectorType: "checkbox" }),
            What: {
                label: nlsHPCC.What, width: 108, sortable: true,
                formatter: function (_, row) {
                    return <Link href={`${searchResultUrl(row)}`}>{row.Type}</Link>;
                }
            },
            Reason: { label: nlsHPCC.Where, width: 108, sortable: true },
            Summary: {
                label: nlsHPCC.Who, sortable: true,
                formatter: function (Summary, row) {
                    return <Link href={`${searchResultUrl(row)}`}>{Summary}</Link>;
                }
            }
        }
    });

    const refreshData = React.useCallback(() => {
        search.searchAll(searchText).then(results => {
            const _data = [];
            const matchedIds: { [key: string]: boolean } = {};
            results.forEach(resultCategory => {
                if (resultCategory) {
                    resultCategory?.forEach(result => {
                        if (!matchedIds[result.id]) {
                            _data.push(result);
                            matchedIds[result.id] = true;
                        }
                    });
                }
            });
            setSearchCount(_data.length);
            setData(_data);
        });
    }, [search, searchText]);

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
                    window.location.href = `#/workunits/${selection[0].Wuid}`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(searchResultUrl(selection[i]), "_blank");
                    }
                }
            }
        },
    ], [refreshData, selection, uiState.hasSelection]);

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
            <PivotItem itemKey="all" headerText={nlsHPCC.All} itemCount={data.length} />
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
