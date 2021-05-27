import * as React from "react";
import { INavLink, INavLinkGroup, INavStyles, Nav } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { useFavorites } from "../hooks/favorite";
import { hashHistory } from "../util/history";

const navLinkGroups: INavLinkGroup[] = [
    {
        name: "Favorites",
        links: []
    },
    {
        name: "History",
        links: []
    },
    {
        name: "Home",
        links: [
            { url: "#/activities", name: nlsHPCC.Activities },
            { url: "#/activities/legacy", name: `${nlsHPCC.Activities} (L)` },
            { url: "#/clusters", name: nlsHPCC.TargetClusters },
            { url: "#/events", name: nlsHPCC.EventScheduler }
        ]
    },
    {
        name: "ECL",
        links: [
            { url: "#/workunits", name: nlsHPCC.Workunits },
            { url: "#/workunits/dashboard", name: `${nlsHPCC.Workunits} (D)` },
            { url: "#/workunits/legacy", name: `${nlsHPCC.Workunits} (L)` },
            { url: "#/play", name: nlsHPCC.Playground },
        ]
    },
    {
        name: "Files",
        links: [
            { url: "#/files", name: nlsHPCC.LogicalFiles },
            { url: "#/files/legacy", name: `${nlsHPCC.LogicalFiles} (L)` },
            { url: "#/landingzone", name: nlsHPCC.LandingZones },
            { url: "#/dfuworkunits", name: nlsHPCC.Workunits },
            { url: "#/dfuworkunits/legacy", name: `${nlsHPCC.Workunits} (L)` },
            { url: "#/xref", name: nlsHPCC.XRef },
        ]
    },
    {
        name: "Published Queries",
        links: [
            { url: "#/queries", name: nlsHPCC.Queries },
            { url: "#/queries/legacy", name: `${nlsHPCC.Queries} (L)` },
            { url: "#/packagemaps", name: nlsHPCC.PackageMaps },
        ]
    },
    {
        name: "Operations",
        links: [
            { url: "#/topology", name: nlsHPCC.Topology },
            { url: "#/diskusage", name: nlsHPCC.DiskUsage },
            { url: "#/clusters2", name: nlsHPCC.TargetClusters },
            { url: "#/processes", name: nlsHPCC.ClusterProcesses },
            { url: "#/servers", name: nlsHPCC.SystemServers },
            { url: "#/security", name: nlsHPCC.Security },
            { url: "#/monitoring", name: nlsHPCC.Monitoring },
            { url: "#/esdl", name: nlsHPCC.DESDL },
            { url: "#/elk", name: nlsHPCC.LogVisualization },
        ]
    }
];
navLinkGroups.forEach(group => {
    group.links.forEach(link => {
        link.key = link.url.substr(1);
    });
});

const navStyles = (width: number, height: number): Partial<INavStyles> => {
    return {
        root: {
            width,
            height,
            boxSizing: "border-box",
            border: "1px solid #eee",
            overflow: "auto",
        }
    };
};

interface DevMenuProps {
    location: string
}

const FIXED_WIDTH = 240;

export const DevMenu: React.FunctionComponent<DevMenuProps> = ({
    location
}) => {

    const [favorites] = useFavorites();
    const [menu, setMenu] = React.useState<INavLinkGroup[]>([...navLinkGroups]);

    React.useEffect(() => {
        navLinkGroups[0].links = Object.keys(favorites).map((key): INavLink => {
            return { url: key, name: key };
        });
        setMenu([...navLinkGroups]);
    }, [favorites]);

    React.useEffect(() => {
        return hashHistory.listen((location, action) => {
            navLinkGroups[1].links = hashHistory.recent().map((row): INavLink => {
                return { url: `#${row.pathname + row.search}`, name: row.pathname };
            });
            setMenu([...navLinkGroups]);
        });
    }, []);

    return <SizeMe monitorHeight>{({ size }) =>
        <div style={{ width: `${FIXED_WIDTH}px`, height: "100%", position: "relative" }}>
            <div style={{ position: "absolute" }}>
                <Nav groups={menu} selectedKey={location} styles={navStyles(FIXED_WIDTH, size.height)} />
            </div>
        </div>
    }
    </SizeMe>;
};
