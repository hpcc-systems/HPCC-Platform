import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, MessageBar, MessageBarType } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import { SizeMe } from "react-sizeme";
import * as parser from "dojox/xml/parser";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import * as WsPackageMaps from "src/WsPackageMaps";
import nlsHPCC from "src/nlsHPCC";
import { pushUrl } from "../util/history";
import { ShortVerticalDivider } from "./Common";
import { AddPackageMapPart } from "./forms/AddPackageMapPart";
import { DojoGrid, selector } from "./DojoGrid";
import { HolyGrail } from "../layouts/HolyGrail";

const logger = scopedLogger("../components/PackageMapParts.tsx");

const defaultUIState = {
    hasSelection: false
};

interface PackageMapPartsProps {
    name: string;
}

export const PackageMapParts: React.FunctionComponent<PackageMapPartsProps> = ({
    name,
}) => {

    const [grid, setGrid] = React.useState<any>(undefined);
    const [_package, setPackage] = React.useState<any>(undefined);
    const [showAddPartForm, setShowAddPartForm] = React.useState(false);
    const [selection, setSelection] = React.useState([]);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");

    //  Grid ---
    const gridStore = useConst(new Observable(new Memory("Part")));
    const gridSort = useConst([{ attribute: "Part", "descending": false }]);
    const gridQuery = useConst({});
    const gridColumns = useConst({
        col1: selector({ width: 27, selectorType: "checkbox" }),
        Part: {
            label: nlsHPCC.Parts,
            formatter: function (part, row) {
                return `<a href="#/packagemaps/${name}/parts/${part}" class='dgrid-row-url'>${part}</a>`;
            }
        },
    });

    const refreshTable = React.useCallback((clearSelection = false) => {
        grid?.set("query", gridQuery);
        if (clearSelection) {
            grid?.clearSelection();
        }
    }, [grid, gridQuery]);

    //  Command Bar ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "add", text: nlsHPCC.Add,
            onClick: () => setShowAddPartForm(true)
        },
        {
            key: "delete", text: nlsHPCC.RemovePart, disabled: !uiState.hasSelection,
            onClick: () => {
                if (confirm(nlsHPCC.YouAreAboutToDeleteThisPart)) {
                    selection.forEach((item, idx) => {
                        WsPackageMaps.RemovePartFromPackageMap({
                            request: {
                                PackageMap: name.split("::")[1],
                                Target: _package.Target,
                                PartName: item.Part
                            }
                        })
                            .then(({ RemovePartFromPackageMapResponse, Exceptions }) => {
                                if (RemovePartFromPackageMapResponse?.status?.Code === 0) {
                                    gridStore.remove(item.Part);
                                    refreshTable();
                                } else if (Exceptions?.Exception.length > 0) {
                                    setShowError(true);
                                    setErrorMessage(Exceptions?.Exception[0].Message);
                                }
                            })
                            .catch(logger.error)
                            ;
                    });
                }
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "getPart", text: nlsHPCC.GetPart, disabled: !uiState.hasSelection,
            onClick: () => {
                if (selection.length === 1) {
                    pushUrl(`/packagemaps/${name}/parts/${selection[0].Part}`);
                } else {
                    selection.forEach(item => {
                        window.open(`#/packagemaps/${name}/parts/${item.Part}`, "_blank");
                    });
                }
            }
        },
    ], [_package, gridStore, name, refreshTable, selection, uiState.hasSelection]);

    React.useEffect(() => {
        WsPackageMaps.getPackageMapById({ packageMap: name })
            .then(({ GetPackageMapByIdResponse }) => {
                const xml = parser.parse(GetPackageMapByIdResponse?.Info);
                const parts = [...xml.getElementsByTagName("Part")].map(part => {
                    return {
                        Part: part.attributes[0].nodeValue
                    };
                });
                gridStore.setData(parts);
                refreshTable();
            })
            .catch(logger.error)
            ;
    }, [gridStore, name, refreshTable]);

    React.useEffect(() => {
        WsPackageMaps.PackageMapQuery({})
            .then(({ ListPackagesResponse }) => {
                const __package = ListPackagesResponse?.PackageMapList?.PackageListMapData.filter(item => item.Id === name)[0];
                setPackage(__package);
            })
            .catch(logger.error)
            ;
    }, [name]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        for (let i = 0; i < selection.length; ++i) {
            state.hasSelection = true;
        }
        setUIState(state);
    }, [selection]);

    return <>
        {showError &&
            <MessageBar messageBarType={MessageBarType.error} isMultiline={false} onDismiss={() => setShowError(false)} dismissButtonAriaLabel="Close">
                {errorMessage}
            </MessageBar>
        }
        <SizeMe monitorHeight>{({ size }) =>
            <HolyGrail
                header={<CommandBar items={buttons} />}
                main={
                    <DojoGrid store={gridStore} sort={gridSort} query={gridQuery} columns={gridColumns} setGrid={setGrid} setSelection={setSelection} />
                }
            />
        }</SizeMe>
        <AddPackageMapPart
            showForm={showAddPartForm} setShowForm={setShowAddPartForm} store={gridStore}
            refreshTable={refreshTable} target={_package?.Target} packageMap={_package?.Id.split("::")[1]}
        />
    </>;
};