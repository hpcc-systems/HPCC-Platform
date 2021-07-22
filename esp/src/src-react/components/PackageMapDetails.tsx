import * as React from "react";
import { CommandBar, ICommandBarItemProps, Pivot, PivotItem, Sticky, StickyPositionType } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import * as WsPackageMaps from "src/WsPackageMaps";
import { pivotItemStyle } from "../layouts/pivot";
import { pushUrl } from "../util/history";
import { PackageMapParts } from "./PackageMapParts";
import { TableGroup } from "./forms/Groups";
import { XMLSourceEditor } from "./SourceEditor";

const logger = scopedLogger("src-react/components/PackageMapDetails.tsx");

interface PackageMapDetailsProps {
    name: string;
    tab?: string;
}

export const PackageMapDetails: React.FunctionComponent<PackageMapDetailsProps> = ({
    name,
    tab = "summary"
}) => {

    const [_package, setPackage] = React.useState<any>();
    const [isActive, setIsActive] = React.useState(false);
    const [xml, setXml] = React.useState("");

    React.useEffect(() => {
        WsPackageMaps.PackageMapQuery({})
            .then(({ ListPackagesResponse }) => {
                const __package = ListPackagesResponse?.PackageMapList?.PackageListMapData.filter(item => item.Id === name)[0];
                setPackage(__package);
                setIsActive(__package.Active);
            })
            .catch(logger.error)
            ;
        WsPackageMaps.getPackageMapById({ packageMap: name })
            .then(({ GetPackageMapByIdResponse }) => {
                setXml(GetPackageMapByIdResponse?.Info);
            })
            .catch(logger.error)
            ;
    }, [name]);

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "activate", text: nlsHPCC.Activate, disabled: isActive,
            onClick: () => {
                WsPackageMaps.activatePackageMap({
                    request: {
                        Target: _package?.Target,
                        Process: _package?.Process,
                        PackageMap: _package?.Id
                    }
                })
                    .then(({ ActivatePackageResponse }) => {
                        if (ActivatePackageResponse?.status?.Code === 0) {
                            setIsActive(true);
                        }
                    })
                    .catch(logger.error)
                    ;
            }
        },
        {
            key: "deactivate", text: nlsHPCC.Deactivate, disabled: !isActive,
            onClick: () => {
                WsPackageMaps.deactivatePackageMap({
                    request: {
                        Target: _package?.Target,
                        Process: _package?.Process,
                        PackageMap: _package?.Id
                    }
                })
                    .then(({ DeActivatePackageResponse }) => {
                        if (DeActivatePackageResponse?.status?.Code === 0) {
                            setIsActive(false);
                        }
                    })
                    .catch(logger.error)
                    ;
            }
        },
        {
            key: "delete", text: nlsHPCC.Delete,
            onClick: () => {
                if (confirm(nlsHPCC.DeleteThisPackage)) {
                    WsPackageMaps.deletePackageMap({
                        request: {
                            Target: _package?.Target,
                            Process: _package?.Process,
                            PackageMap: _package?.Id
                        }
                    })
                        .then(({ DeletePackageResponse, Exceptions }) => {
                            if (DeletePackageResponse?.status?.Code === 0) {
                                pushUrl("/packagemaps");
                            } else if (Exceptions) {
                                logger.error(Exceptions.Exception[0].Message);
                            }
                        })
                        .catch(logger.error)
                        ;
                }
            }
        },
    ], [_package, isActive]);

    return <SizeMe monitorHeight>{({ size }) =>
        <Pivot
            overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab}
            onLinkClick={evt => {
                pushUrl(`/packagemaps/${name}/${evt.props.itemKey}`);
            }}
        >
            <PivotItem headerText={name} itemKey="summary" style={pivotItemStyle(size)} >
                <Sticky stickyPosition={StickyPositionType.Header}>
                    <CommandBar items={buttons} />
                </Sticky>
                <TableGroup fields={{
                    "target": { label: nlsHPCC.ID, type: "string", value: _package?.Id, readonly: true },
                    "process": { label: nlsHPCC.ClusterName, type: "string", value: _package?.Process, readonly: true },
                    "active": { label: nlsHPCC.Active, type: "string", value: isActive ? "true" : "false", readonly: true },
                }} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.XML} itemKey="xml" style={pivotItemStyle(size, 0)}>
                <XMLSourceEditor text={xml} readonly={true} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.title_PackageParts} itemKey="parts" style={pivotItemStyle(size, 0)}>
                <PackageMapParts name={name} />
            </PivotItem>
        </Pivot>
    }</SizeMe>;
};