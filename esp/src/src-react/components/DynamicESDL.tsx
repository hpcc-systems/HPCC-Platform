import * as React from "react";
import { Pivot, PivotItem } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { useBuildInfo } from "../hooks/platform";
import { DESDLBindings } from "./DESDLBindings";
import { DESDLDefinitions } from "./DESDLDefinitions";
import { pivotItemStyle } from "../layouts/pivot";
import { pushUrl } from "../util/history";

interface ESDLBindingProps {
    tab?: string;
}

export const DynamicESDL: React.FunctionComponent<ESDLBindingProps> = ({
    tab = "bindings"
}) => {

    const [, { opsCategory }] = useBuildInfo();

    return <>
        <SizeMe monitorHeight>{({ size }) =>
            <Pivot
                overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab}
                onLinkClick={evt => pushUrl(`/${opsCategory}/desdl/${evt.props.itemKey}`)}
            >
                <PivotItem headerText={nlsHPCC.title_DESDL} itemKey="bindings" style={pivotItemStyle(size)} >
                    <DESDLBindings />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.Definitions} itemKey="definitions" style={pivotItemStyle(size, 0)}>
                    <DESDLDefinitions />
                </PivotItem>
            </Pivot>
        }</SizeMe>
    </>;
};