import * as React from "react";
import { DefaultButton, Dialog, DialogFooter, DialogType, Pivot, PivotItem } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { Bar } from "@hpcc-js/chart";
import { fetchStats } from "src/KeyValStore";
import nlsHPCC from "src/nlsHPCC";
import { TpGetServerVersion } from "src/WsTopology";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { TableGroup } from "./forms/Groups";

const dialogContentProps = {
    type: DialogType.largeHeader,
    title: nlsHPCC.AboutHPCCSystems
};

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

    const [version, setVersion] = React.useState("");
    const [browser, setBrowser] = React.useState([]);
    const [os, setOS] = React.useState([]);

    React.useEffect(() => {
        TpGetServerVersion().then(version => {
            setVersion(version);
        });
        fetchStats().then(response => {
            setBrowser(response.browser);
            setOS(response.os);
        });
    }, []);

    const browserChart = useConst(new Bar().columns([nlsHPCC.Client, nlsHPCC.Count]));
    React.useEffect(() => {
        browserChart
            .data(browser)
            .lazyRender()
            ;
    }, [browser, browserChart]);
    const osChart = useConst(new Bar().columns([nlsHPCC.Client, nlsHPCC.Count]));
    React.useEffect(() => {
        osChart
            .data(os)
            .lazyRender()
            ;
    }, [os, osChart]);

    return <Dialog hidden={!show} onDismiss={onClose} dialogContentProps={dialogContentProps} minWidth="640px">
        <Pivot>
            <PivotItem itemKey="about" headerText={nlsHPCC.About}>
                <div style={{ minHeight: "208px", paddingTop: "32px" }}>
                    <TableGroup fields={{
                        platformVersion: { label: `${nlsHPCC.Platform}:`, type: "string", value: version || "???", readonly: true },
                        eclwatchVersion: { label: "ECL Watch:", type: "string", value: eclwatchVersion, readonly: true },
                    }}>
                    </TableGroup>
                </div>
            </PivotItem>
            <PivotItem itemKey="browser" headerText={nlsHPCC.BrowserStats} alwaysRender>
                <AutosizeHpccJSComponent widget={browserChart} fixedHeight="240px" />
            </PivotItem>
            <PivotItem itemKey="os" headerText={nlsHPCC.OSStats} alwaysRender>
                <AutosizeHpccJSComponent widget={osChart} fixedHeight="240px" />
            </PivotItem>
        </Pivot>
        <DialogFooter>
            <DefaultButton onClick={onClose} text={nlsHPCC.OK} />
        </DialogFooter>
    </Dialog>;
};
