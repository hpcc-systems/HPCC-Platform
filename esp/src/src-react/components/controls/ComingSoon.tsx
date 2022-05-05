import * as React from "react";
import { IStyle, Toggle } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { useUserStore } from "../../hooks/store";

interface ComingSoon {
    defaultValue: boolean;
    style?: IStyle;
}

export const ComingSoon: React.FunctionComponent<ComingSoon> = ({
    defaultValue = false,
    style
}) => {
    const [modernMode, setModernMode] = useUserStore("ModernMode", String(defaultValue));

    const { legacyIndex, modernIndex } = React.useMemo(() => {
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

            ["#/stub/OPS-DL/Topology", "#/topology-old"],
            ["#/stub/OPS-DL/DiskUsage", "#/diskusage"],
            ["#/stub/OPS-DL/TargetClustersQuery", "#/clusters"],
            ["#/stub/OPS-DL/ClusterProcessesQuery", "#/processes"],
            ["#/stub/OPS-DL/SystemServersQuery", "#/servers"],
            ["#/stub/OPS-DL/Permissions", "#/security"],
            ["#/stub/OPS-DL/Monitoring", "#/monitoring"],
            ["#/stub/OPS-DL/DESDL", "#/desdl"],
            ["#/stub/OPS-DL/LogVisualization", "#/topology-old"],
            ["#/stub/OPS", "#/topology-old"],
        ].forEach(row => {
            legacyIndex[row[0]] = row[1];
            modernIndex[row[1]] = row[0];
        });
        return { legacyIndex, modernIndex };
    }, []);

    const onChangeCallback = React.useCallback((ev: React.MouseEvent<HTMLElement>, checked: boolean) => {
        setModernMode(checked ? String(true) : String(false));
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
                    bookmark = modernIndex[key];
                    break;
                }
            }
            window.location.replace(`/esp/files/stub.htm${bookmark}`);
        }
    }, [legacyIndex, modernIndex, setModernMode]);

    return <Toggle label={nlsHPCC.TechPreview} checked={(modernMode ?? String(defaultValue)) !== String(false)} onText={nlsHPCC.On} offText={nlsHPCC.Off} onChange={onChangeCallback} styles={{ label: style }} />;
};
