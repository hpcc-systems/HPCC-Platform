import * as React from "react";
import { Pivot, PivotItem } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import { pushUrl } from "../util/history";
import { Groups } from "./Groups";
import { Users } from "./Users";
import { pivotItemStyle } from "../layouts/pivot";
import nlsHPCC from "src/nlsHPCC";

interface SecurityProps {
    filter?: object;
    tab?: string;
}

export const Security: React.FunctionComponent<SecurityProps> = ({
    filter,
    tab = "users"
}) => {

    return <>
        <SizeMe monitorHeight>{({ size }) =>
            <Pivot
                overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab}
                onLinkClick={evt => pushUrl(`/security/${evt.props.itemKey}`)}
            >
                <PivotItem headerText={nlsHPCC.Users} itemKey="users" style={pivotItemStyle(size)}>
                    <Users filter={filter} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.Groups} itemKey="groups" style={pivotItemStyle(size)}>
                    <Groups filter={filter} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.Permissions} itemKey="permissions" style={pivotItemStyle(size)}>
                </PivotItem>
            </Pivot>
        }</SizeMe>
    </>;

};