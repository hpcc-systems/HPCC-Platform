import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { SizeMe } from "react-sizeme";
import * as parser from "dojox/xml/parser";
import * as WsPackageMaps from "src/WsPackageMaps";
import nlsHPCC from "src/nlsHPCC";
import { useConfirm } from "../hooks/confirm";
import { pushUrl } from "../util/history";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { ShortVerticalDivider } from "./Common";
import { AddPackageMapPart } from "./forms/AddPackageMapPart";
import { selector } from "./DojoGrid";

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

    const [_package, setPackage] = React.useState<any>(undefined);
    const [showAddPartForm, setShowAddPartForm] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            col1: selector({ width: 27, selectorType: "checkbox" }),
            Part: {
                label: nlsHPCC.Parts,
                formatter: (part, row) => {
                    return <Link href={`#/packagemaps/${name}/parts/${part}`}>{part}</Link>;
                }
            },
        };
    }, [name]);

    const refreshData = React.useCallback(() => {
        WsPackageMaps.getPackageMapById({ packageMap: name })
            .then(({ GetPackageMapByIdResponse }) => {
                const xml = parser.parse(GetPackageMapByIdResponse?.Info);
                const parts = [...xml.getElementsByTagName("Part")].map(part => {
                    return {
                        Part: part.attributes[0].nodeValue
                    };
                });
                setData(parts);
            })
            .catch(err => logger.error(err))
            ;
    }, [name]);

    React.useEffect(() => {
        refreshData();
    }, [refreshData]);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.YouAreAboutToDeleteThisPart,
        onSubmit: React.useCallback(() => {
            const requests = [];
            selection.forEach((item, idx) => {
                requests.push(
                    WsPackageMaps.RemovePartFromPackageMap({
                        request: {
                            PackageMap: name.split("::")[1],
                            Target: _package?.Target,
                            PartName: item.Part
                        }
                    })
                );
                Promise
                    .all(requests)
                    .then(() => refreshData())
                    .catch(err => logger.error(err))
                    ;
            });
        }, [_package?.Target, name, refreshData, selection])
    });

    //  Command Bar ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "add", text: nlsHPCC.Add,
            onClick: () => setShowAddPartForm(true)
        },
        {
            key: "delete", text: nlsHPCC.RemovePart, disabled: !uiState.hasSelection,
            onClick: () => setShowDeleteConfirm(true)
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
    ], [name, refreshData, selection, setShowDeleteConfirm, uiState.hasSelection]);

    const copyButtons = useCopyButtons(columns, selection, "packageMapParts");

    React.useEffect(() => {
        WsPackageMaps.PackageMapQuery({})
            .then(({ ListPackagesResponse }) => {
                const __package = ListPackagesResponse?.PackageMapList?.PackageListMapData.filter(item => item.Id === name)[0];
                setPackage(__package);
            })
            .catch(err => logger.error(err))
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
        <SizeMe monitorHeight>{({ size }) =>
            <HolyGrail
                header={<CommandBar items={buttons} farItems={copyButtons} />}
                main={
                    <FluentGrid
                        data={data}
                        primaryID={"Part"}
                        sort={{ attribute: "Part", descending: false }}
                        columns={columns}
                        setSelection={setSelection}
                        setTotal={setTotal}
                        refresh={refreshTable}
                    ></FluentGrid>}
            />
        }</SizeMe>
        <AddPackageMapPart
            showForm={showAddPartForm} setShowForm={setShowAddPartForm}
            refreshData={refreshData} target={_package?.Target} packageMap={_package?.Id.split("::")[1]}
        />
        <DeleteConfirm />
    </>;
};