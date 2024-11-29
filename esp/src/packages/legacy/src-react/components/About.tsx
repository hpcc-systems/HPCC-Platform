import * as React from "react";
import { DefaultButton, Dialog, DialogFooter, DialogType, Pivot, PivotItem, Spinner } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { useCheckFeatures, fetchLatestReleases } from "../hooks/platform";
import { TableGroup } from "./forms/Groups";
import { Fields } from "./forms/Fields";

const dialogContentProps = {
    type: DialogType.largeHeader,
    title: nlsHPCC.AboutHPCCSystems
};

interface AboutProps {
    eclwatchVersion: string;
    show?: boolean;
    onClose?: () => void;
}

const dateOptions: Intl.DateTimeFormatOptions = { year: "numeric", month: "long", day: "numeric" };

export const About: React.FunctionComponent<AboutProps> = ({
    eclwatchVersion = "",
    show = false,
    onClose = () => { }
}) => {

    const [loaded, setLoaded] = React.useState(0);
    const [latestReleases, setLatestReleases] = React.useState<Fields>({});

    const features = useCheckFeatures();

    React.useEffect(() => {
        if (show && loaded === 0) {
            setLoaded(1);
            fetchLatestReleases().then(releases => {
                const fields: Fields = {};
                releases.forEach(release => {
                    fields[release.tag_name] = {
                        label: release.tag_name,
                        type: "link",
                        value: release.html_url.replace("github.com/hpcc-systems/HPCC-Platform", "..."),
                        href: release.html_url,
                        newTab: true,
                        readonly: true
                    };
                });
                setLoaded(2);
                setLatestReleases(fields);
            });
        }
    }, [loaded, show]);

    return <Dialog hidden={!show} onDismiss={onClose} dialogContentProps={dialogContentProps} minWidth="640px">
        <Pivot>
            <PivotItem itemKey="about" headerText={nlsHPCC.About}>
                <div style={{ minHeight: "208px", paddingTop: "32px" }}>
                    <TableGroup width="100%" fields={{
                        platformVersion: { label: `${nlsHPCC.Platform}:`, type: "string", value: features?.version || "???", readonly: true },
                        platformDate: { label: `${nlsHPCC.BuildDate}:`, type: "string", value: features?.timestamp?.toLocaleDateString(undefined, dateOptions) || "???", readonly: true },
                        eclwatchVersion: { label: "ECL Watch:", type: "string", value: eclwatchVersion, readonly: true },
                    }}>
                    </TableGroup>
                </div>
            </PivotItem>
            <PivotItem itemKey="latest" headerText={nlsHPCC.LatestReleases}>
                {
                    loaded >= 2 &&
                    <div style={{ minHeight: "208px", overflow: "hidden", paddingTop: "32px" }}>
                        <TableGroup width="100%" fields={latestReleases}>
                        </TableGroup>
                    </div> ||
                    <div style={{ minHeight: "208px", paddingTop: "32px" }}>
                        <Spinner labelPosition="bottom" label={nlsHPCC.Loading} />
                    </div>
                }
            </PivotItem>
        </Pivot>
        <DialogFooter>
            <DefaultButton onClick={onClose} text={nlsHPCC.OK} />
        </DialogFooter>
    </Dialog>;
};
