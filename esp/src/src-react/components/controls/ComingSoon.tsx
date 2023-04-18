import * as React from "react";
import { IStyle, Toggle } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { ModernMode } from "src/BuildInfo";
import { useUserStore } from "../../hooks/store";
import { useBuildInfo } from "../../hooks/platform";

const legacyIndex = {};
const modernIndex = {};
[
    ["#/stub/Main-DL/Activity", "#/activities"],
    ["#/stub/Main-DL/Event", "#/events"],
    ["#/stub/Main-DL/Search", "#/search"],
    ["#/stub/Main", "#/activities"],

    ["#/stub/ECL-DL/Workunits", "#/workunits"],
    ["#/stub/ECL-DL/Playground", "#/play"],
    ["#/stub/ECL", "#/workunits"],

    ["#/stub/Files-DL/LogicalFiles", "#/files"],
    ["#/stub/Files-DL/LandingZones", "#/landingzone"],
    ["#/stub/Files-DL/Workunits", "#/dfuworkunits"],
    ["#/stub/Files-DL/Xref", "#/xref"],
    ["#/stub/Files", "#/files"],

    ["#/stub/RoxieQueries-DL/Queries", "#/queries"],
    ["#/stub/RoxieQueries-DL/PackageMaps", "#/packagemaps"],
    ["#/stub/RoxieQueries", "#/queries"],

    ["#/stub/OPS-DL/Topology", "#/operations"],
    ["#/stub/OPS-DL/DiskUsage", "#/operations/diskusage"],
    ["#/stub/OPS-DL/TargetClustersQuery", "#/operations/clusters"],
    ["#/stub/OPS-DL/ClusterProcessesQuery", "#/operations/processes"],
    ["#/stub/OPS-DL/SystemServersQuery", "#/operations/servers"],
    ["#/stub/OPS-DL/Permissions", "#/opsCategory/security"],
    ["#/stub/OPS-DL/DESDL", "#/opsCategory/desdl"],
    ["#/stub/OPS-DL/LogVisualization", "#/operations"],
    ["#/stub/OPS", "#/operations"],
].forEach(row => {
    legacyIndex[row[0]] = row[1];
    modernIndex[row[1]] = row[0];
});

export function switchTechPreview(checked: boolean, opsCategory: string) {
    let bookmark = "";
    if (checked) {
        for (const key in legacyIndex) {
            if (window.location.hash.indexOf(key) === 0) {
                bookmark = legacyIndex[key];
                break;
            }
        }
        window.location.replace(`/esp/files/index.html${bookmark}`);
    } else {
        for (const key in modernIndex) {
            if (window.location.hash.indexOf(key) === 0) {
                bookmark = modernIndex[key].replace("opsCategory", opsCategory);
                break;
            }
        }
        window.location.replace(`/esp/files/stub.htm${bookmark}`);
    }
}

interface ComingSoon {
    defaultValue: boolean;
    style?: IStyle;
    value?: boolean;
}

export const ComingSoon: React.FunctionComponent<ComingSoon> = ({
    defaultValue = false,
    style,
    value
}) => {

    const [, { opsCategory }] = useBuildInfo();
    const [modernMode, setModernMode] = useUserStore(ModernMode, String(defaultValue));

    React.useEffect(() => {
        if (value !== undefined) {
            setModernMode(String(value));
            switchTechPreview(value, opsCategory);
        }
    }, [opsCategory, setModernMode, value]);

    const onChangeCallback = React.useCallback((ev: React.MouseEvent<HTMLElement>, checked: boolean) => {
        setModernMode(checked ? String(true) : String(false));
        switchTechPreview(checked, opsCategory);
    }, [opsCategory, setModernMode]);

    return <Toggle label="ECL Watch v9" checked={(modernMode ?? String(defaultValue)) !== String(false)} onText={nlsHPCC.On} offText={nlsHPCC.Off} onChange={onChangeCallback} styles={{ label: style }} />;
};
