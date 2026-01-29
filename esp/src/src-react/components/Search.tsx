import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link, ProgressIndicator } from "@fluentui/react";
import { SelectTabData, SelectTabEvent, Tab, TabList } from "@fluentui/react-components";
import { useConst } from "@fluentui/react-hooks";
import { ESPSearch } from "src/ESPSearch";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { ShortVerticalDivider } from "./Common";
import { Workunits } from "./Workunits";
import { Files } from "./Files";
import { Queries } from "./Queries";
import { DFUWorkunits } from "./DFUWorkunits";
import { Count } from "./controls/TabbedPanes/Count";

const defaultUIState = {
    hasSelection: false,
};

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
        case "Super File":
            url = "#/files/" + result._name;
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

    const progress = useConst(() => ({ value: 0 }));

    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [searchCount, setSearchCount] = React.useState(0);

    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Search
    const search = useConst(() => new ESPSearch(() => { progress.value++; }));

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            col1: { width: 27, selectorType: "checkbox" },
            What: {
                label: nlsHPCC.What, width: 108, sortable: true,
                formatter: (_, row) => {
                    return <Link href={`${searchResultUrl(row)}`}>{row.Type}</Link>;
                }
            },
            Reason: { label: nlsHPCC.Where, width: 108, sortable: true },
            Summary: {
                label: nlsHPCC.Who, sortable: true,
                formatter: (Summary, row) => {
                    return <Link href={`${searchResultUrl(row)}`}>{Summary}</Link>;
                }
            }
        };
    }, []);

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
                    window.location.href = searchResultUrl(selection[0]);
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(searchResultUrl(selection[i]), "_blank");
                    }
                }
            }
        },
    ], [refreshData, selection, uiState.hasSelection]);

    const copyButtons = useCopyButtons(columns, selection, "search");

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        for (let i = 0; i < selection.length; ++i) {
            state.hasSelection = true;
        }
        setUIState(state);
    }, [selection]);

    const [selectedKey, setSelectedKey] = React.useState("all");

    const onTabSelect = React.useCallback((_: SelectTabEvent, data: SelectTabData) => {
        setSelectedKey(data.value as string);
    }, []);

    return <HolyGrail
        header={<TabList selectedValue={selectedKey} onTabSelect={onTabSelect} size="medium">
            <Tab value="all">{nlsHPCC.All}<Count value={data.length} /></Tab>
            <Tab value="ecl" disabled={search.eclStore.data.length === 0}>{nlsHPCC.ECLWorkunit}<Count value={search.eclStore.data.length} /></Tab>
            <Tab value="dfu" disabled={search.dfuStore.data.length === 0}>{nlsHPCC.DFUWorkunit}<Count value={search.dfuStore.data.length} /></Tab>
            <Tab value="file" disabled={search.fileStore.data.length === 0}>{nlsHPCC.LogicalFile}<Count value={search.fileStore.data.length} /></Tab>
            <Tab value="query" disabled={search.queryStore.data.length === 0}>{nlsHPCC.Query}<Count value={search.queryStore.data.length} /></Tab>
        </TabList>}
        main={selectedKey === "all" ? <HolyGrail
            header={<>
                <CommandBar items={buttons} farItems={copyButtons} />
                <ProgressIndicator progressHidden={searchCount === 0} percentComplete={searchCount === 0 ? 0 : progress.value / searchCount} />
            </>}
            main={<FluentGrid
                data={data}
                primaryID={"__hpcc_id"}
                sort={{ attribute: "__hpcc_id", descending: false }}
                columns={columns}
                setSelection={setSelection}
                setTotal={setTotal}
                refresh={refreshTable}
            ></FluentGrid>}
        /> : selectedKey === "ecl" ?
            <Workunits store={search.eclStore} /> : selectedKey === "dfu" ?
                <DFUWorkunits store={search.dfuStore} /> : selectedKey === "file" ?
                    <Files store={search.fileStore} /> : selectedKey === "query" ?
                        <Queries store={search.queryStore} /> :
                        undefined
        }
    />;
};
