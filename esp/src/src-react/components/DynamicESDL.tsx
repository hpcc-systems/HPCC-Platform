import * as React from "react";
import { SelectTabData, SelectTabEvent, Tab, TabList, makeStyles } from "@fluentui/react-components";
import { SizeMe } from "../layouts/SizeMe";
import nlsHPCC from "src/nlsHPCC";
import { useBuildInfo } from "../hooks/platform";
import { DESDLBindings } from "./DESDLBindings";
import { DESDLDefinitions } from "./DESDLDefinitions";
import { pivotItemStyle } from "../layouts/pivot";
import { pushUrl } from "../util/history";

const useStyles = makeStyles({
    container: {
        height: "100%",
        position: "relative"
    }
});

interface ESDLBindingProps {
    tab?: string;
}

export const DynamicESDL: React.FunctionComponent<ESDLBindingProps> = ({
    tab = "bindings"
}) => {

    const [, { opsCategory }] = useBuildInfo();

    const onTabSelect = React.useCallback((_: SelectTabEvent, data: SelectTabData) => {
        pushUrl(`/${opsCategory}/desdl/${data.value as string}`);
    }, [opsCategory]);

    const styles = useStyles();

    return <>
        <SizeMe>{({ size }) =>
            <div className={styles.container}>
                <TabList selectedValue={tab} onTabSelect={onTabSelect} size="medium">
                    <Tab value="bindings">{nlsHPCC.title_DESDL}</Tab>
                    <Tab value="definitions">{nlsHPCC.Definitions}</Tab>
                </TabList>
                {tab === "bindings" &&
                    <div style={pivotItemStyle(size)}>
                        <DESDLBindings />
                    </div>
                }
                {tab === "definitions" &&
                    <div style={pivotItemStyle(size, 0)}>
                        <DESDLDefinitions />
                    </div>
                }
            </div>
        }</SizeMe>
    </>;
};