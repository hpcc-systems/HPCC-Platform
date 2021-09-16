import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import * as WsPackageMaps from "src/WsPackageMaps";
import { XMLSourceEditor } from "./SourceEditor";

const logger = scopedLogger("../components/PackageMapPartDetails.tsx");

interface PackageMapPartDetailsProps {
    name: string;
    part: string;
}

export const PackageMapPartDetails: React.FunctionComponent<PackageMapPartDetailsProps> = ({
    name,
    part
}) => {

    const [_package, setPackage] = React.useState<any>(undefined);
    const [xml, setXml] = React.useState("");

    React.useEffect(() => {
        WsPackageMaps.PackageMapQuery({})
            .then(({ ListPackagesResponse }) => {
                const __package = ListPackagesResponse?.PackageMapList?.PackageListMapData.filter(item => item.Id === name)[0];
                setPackage(__package);
            })
            .catch(logger.error)
            ;
    }, [name]);

    React.useEffect(() => {
        if (!_package || !name || !part) return;
        WsPackageMaps.GetPartFromPackageMap({
            request: {
                PackageMap: name.split("::")[1],
                Target: _package?.Target,
                PartName: part
            }
        })
            .then(({ GetPartFromPackageMapResponse, Exceptions }) => {
                setXml(GetPartFromPackageMapResponse?.Content);
            })
            .catch(logger.error)
            ;
    }, [_package, name, part]);

    return <XMLSourceEditor text={xml} readonly={true} />;
};