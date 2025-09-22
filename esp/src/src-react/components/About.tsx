import * as React from "react";
import { makeStyles, Dialog, Button, SelectTabData, SelectTabEvent, Spinner, Tab, TabList, TabValue, DialogSurface, DialogBody, DialogTitle, DialogContent, DialogActions, DialogOpenChangeEvent, DialogOpenChangeData } from "@fluentui/react-components";
import nlsHPCC from "src/nlsHPCC";
import { useCheckFeatures, fetchLatestReleases } from "../hooks/platform";
import { TableGroup } from "./forms/Groups";
import { Fields } from "./forms/Fields";

const dateOptions: Intl.DateTimeFormatOptions = { year: "numeric", month: "long", day: "numeric" };

const useStyles = makeStyles({
    dialogSurface: {
        minWidth: "640px",
    },
    tabContent: {
        minHeight: "208px",
        paddingTop: "32px",
    },
    tabContentLatest: {
        minHeight: "208px",
        overflow: "hidden",
        paddingTop: "32px",
    },
});

enum LOAD_RELEASES {
    NOT_LOADED = 0,
    LOADING = 1,
    LOADED = 2
}

interface AboutProps {
    eclwatchVersion: string;
    show?: boolean;
    onClose?: () => void;
}

export const About: React.FunctionComponent<AboutProps> = ({
    eclwatchVersion = "",
    show = false,
    onClose = () => { }
}) => {

    const [activeTab, setActiveTab] = React.useState<TabValue>("about");
    const [loaded, setLoaded] = React.useState<LOAD_RELEASES>(LOAD_RELEASES.NOT_LOADED);
    const [latestReleases, setLatestReleases] = React.useState<Fields>({});

    const features = useCheckFeatures();
    const styles = useStyles();

    React.useEffect(() => {
        if (!show) return;
        if (loaded === LOAD_RELEASES.NOT_LOADED) {
            setLoaded(LOAD_RELEASES.LOADING);
            fetchLatestReleases()
                .then(releases => {
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
                    setLoaded(LOAD_RELEASES.LOADED);
                    setLatestReleases(fields);
                });
        }
    }, [loaded, show]);

    const onTabSelect = React.useCallback((event: SelectTabEvent, data: SelectTabData) => {
        setActiveTab(data.value);
    }, []);

    const onDialogOpenChange = React.useCallback((_: DialogOpenChangeEvent, data: DialogOpenChangeData) => {
        if (!data.open) {
            onClose();
        }
    }, [onClose]);

    return <Dialog open={show} modalType="modal" onOpenChange={onDialogOpenChange}>
        <DialogSurface className={styles.dialogSurface}>
            <DialogBody>
                <DialogTitle>{nlsHPCC.AboutHPCCSystems}</DialogTitle>
                <DialogContent>
                    <TabList selectedValue={activeTab} onTabSelect={onTabSelect}>
                        <Tab value="about">{nlsHPCC.About}</Tab>
                        <Tab value="latest">{nlsHPCC.LatestReleases}</Tab>
                    </TabList>
                    <div>
                        {activeTab === "about" &&
                            <div className={styles.tabContent}>
                                <TableGroup width="100%" fields={{
                                    platformVersion: { label: `${nlsHPCC.Platform}:`, type: "string", value: features?.version || "???", readonly: true },
                                    platformDate: { label: `${nlsHPCC.BuildDate}:`, type: "string", value: features?.timestamp?.toLocaleDateString(undefined, dateOptions) || "???", readonly: true },
                                    eclwatchVersion: { label: "ECL Watch:", type: "string", value: eclwatchVersion, readonly: true },
                                }} />
                            </div>
                        }
                        {activeTab === "latest" && (loaded >= LOAD_RELEASES.LOADED ?
                            <div className={styles.tabContentLatest}>
                                <TableGroup width="100%" fields={latestReleases}>
                                </TableGroup>
                            </div> :
                            <div className={styles.tabContent}>
                                <Spinner labelPosition="below" label={nlsHPCC.Loading} />
                            </div>)
                        }
                    </div>
                </DialogContent>
                <DialogActions>
                    <Button onClick={onClose} appearance="primary" >{nlsHPCC.OK}</Button>
                </DialogActions>
            </DialogBody>
        </DialogSurface>
    </Dialog>;
};
