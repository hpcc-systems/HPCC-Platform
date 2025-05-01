import * as React from "react";
import { Text, ToastTitle, Toast, ToastBody, ToastFooter, ToastTrigger, Link, ToolbarDivider } from "@fluentui/react-components";
import { Level } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";

export interface CustomToasterProps {
    id: string,
    level: Level,
    message: string,
    onDismissAll: () => void
}

export const CustomToaster: React.FunctionComponent<CustomToasterProps> = ({
    id,
    level,
    message,
    onDismissAll
}) => {

    return <Toast>
        <ToastTitle>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", width: "100%" }}>
                <Text weight="bold">{Level[level]}</Text>
                <div style={{ display: "flex", alignItems: "right" }}>
                    <ToastTrigger>
                        <Link>{nlsHPCC.dismiss}</Link>
                    </ToastTrigger>
                    <ToolbarDivider />
                    <Link onClick={onDismissAll}>{nlsHPCC.dismissAll}</Link>
                </div>
            </div>
        </ToastTitle>
        <ToastBody>
            <Text>{message}</Text>
        </ToastBody>
        <ToastFooter>{id}</ToastFooter>
    </Toast>;
};