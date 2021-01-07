import * as React from "react";

export interface HolyGrailProps {
    header?: any;
    left?: any;
    main?: any;
    right?: any;
    footer?: any;
}

export const HolyGrail: React.FunctionComponent<HolyGrailProps> = ({
    header,
    left,
    main,
    right,
    footer
}) => {

    return <div style={{ display: "flex", flexDirection: "column", minWidth: 0, minHeight: "100%", overflow: "hidden" }}>
        <header style={{ flex: "0 0", minWidth: 0 }}>{header}</header>
        <div style={{ flex: "1 1", display: "flex", minWidth: 0 }} >
            <div style={{ flex: "0 2" }}>{left}</div>
            <div style={{ flex: "1 1 auto", minWidth: 0 }}>{main}</div>
            <div style={{ flex: "0 2" }}>{right}</div>
        </div>
        <footer style={{ flex: "0 0", minWidth: 0 }}>{footer}</footer>
    </div>;
};
