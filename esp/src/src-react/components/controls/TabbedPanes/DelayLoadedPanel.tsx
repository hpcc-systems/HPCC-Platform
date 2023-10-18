import * as React from "react";
import { Size, pivotItemStyle } from "../../../layouts/pivot";

export interface DelayLoadedPanelProps {
    visible: boolean;
    size: Size;
    padding?: number;
}

export const DelayLoadedPanel: React.FunctionComponent<DelayLoadedPanelProps> = ({
    visible,
    size,
    padding = 4,
    children
}) => {

    const [loaded, setLoaded] = React.useState(false);
    const [style, setStyle] = React.useState(pivotItemStyle(size, padding));

    React.useEffect(() => {
        if (visible) {
            setLoaded(true);
            setStyle(pivotItemStyle(size, padding));
        }
    }, [padding, size, visible]);

    if (!loaded) return <></>;
    return <div style={{ ...style, display: visible ? "block" : "none" }}>
        {children}
    </div >;
};

