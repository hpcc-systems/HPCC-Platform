import * as React from "react";
import { useId } from "@fluentui/react-hooks";
import { SizeMe } from "../layouts/SizeMe";
import { Widget } from "@hpcc-js/common";

import "src-react-css/layouts/HpccJSAdapter.css";

export interface HpccJSComponentProps {
    widget: Widget;
    width: number;
    height: number;
    debounce?: boolean;
    onReady?: () => void;
}

export const HpccJSComponent: React.FunctionComponent<HpccJSComponentProps> = ({
    widget,
    width,
    height,
    debounce = true,
    onReady
}) => {
    const divID = useId("viz-component-");

    const setDivRef = React.useCallback(node => {
        widget?.target(node);
        if (node) {
            widget?.render();
        }
    }, [widget]);

    React.useEffect(() => {
        if (widget?.target() && !isNaN(width) && !isNaN(height) && width > 0 && height > 0) {
            widget.resize({ width, height });
            if (debounce) {
                widget.lazyRender(() => onReady?.());
            } else {
                widget.render(() => onReady?.());
            }
        }
    }, [debounce, height, onReady, widget, width]);

    return <div ref={setDivRef} id={divID} className="hpcc-js-component" style={{ width, height }}>
    </div>;
};

export interface AutosizeHpccJSComponentProps {
    widget: Widget;
    fixedHeight?: string;
    padding?: number;
    debounce?: boolean;
    hidden?: boolean;
    onReady?: () => void;
    children?: React.ReactNode;
}

export const AutosizeHpccJSComponent: React.FunctionComponent<AutosizeHpccJSComponentProps> = ({
    widget,
    fixedHeight = "100%",
    padding = 0,
    debounce = true,
    hidden = false,
    onReady,
    children
}) => {

    return <SizeMe>{({ size }) => {
        const width = size?.width || padding * 2;
        const height = size?.height || padding * 2;
        return <div style={{ width: "100%", height: hidden ? "1px" : fixedHeight, position: "relative" }}>
            <div style={{ position: "absolute", padding: `${padding}px`, display: hidden ? "none" : "block" }}>
                <HpccJSComponent widget={widget} debounce={debounce} width={width - padding * 2} height={height - padding * 2} onReady={onReady} />
            </div>
            {
                children ?
                    <div style={{ position: "absolute", padding: `${padding}px`, display: hidden ? "none" : "block" }}>
                        {children}
                    </div> :
                    <></>
            }
        </div>;
    }}
    </SizeMe>;
};

export interface AutosizeComponentProps {
    fixedHeight?: string;
    padding?: number;
    hidden?: boolean;
    children?: React.ReactNode;
}

export const AutosizeComponent: React.FunctionComponent<AutosizeComponentProps> = ({
    fixedHeight = "100%",
    padding = 0,
    hidden = false,
    children
}) => {

    return <SizeMe hidden={hidden}>{({ size }) => {
        const width = size?.width || padding * 2;
        const height = size?.height || padding * 2;
        return <div style={{ width: "100%", height: fixedHeight, position: "relative" }}>
            <div style={{ position: "absolute", padding: `${padding}px` }}>
                <div style={{ width: width - padding * 2, height: height - padding * 2, display: "flex", alignItems: "center", justifyContent: "center" }} >
                    {children}
                </div>
            </div>
        </div>;
    }}
    </SizeMe>;
};
