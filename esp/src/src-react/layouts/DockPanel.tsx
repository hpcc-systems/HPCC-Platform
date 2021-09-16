import * as React from "react";
import * as ReactDOM from "react-dom";
import { PartialTheme, ThemeProvider, useTheme } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { HTMLWidget, Widget } from "@hpcc-js/common";
import { DockPanel as HPCCDockPanel, IClosable } from "@hpcc-js/phosphor";
import { useUserStore } from "../hooks/store";
import { lightTheme } from "../themes";
import { AutosizeHpccJSComponent } from "./HpccJSAdapter";

export class ReactWidget extends HTMLWidget {

    protected _theme: PartialTheme = lightTheme;
    protected _children = <div></div>;

    protected _div;

    constructor() {
        super();
    }

    theme(): PartialTheme;
    theme(_: PartialTheme): this;
    theme(_?: PartialTheme): this | PartialTheme {
        if (arguments.length === 0) return this._theme;
        this._theme = _;
        return this;
    }

    children(): JSX.Element;
    children(_: JSX.Element): this;
    children(_?: JSX.Element): this | JSX.Element {
        if (arguments.length === 0) return this._children;
        this._children = _;
        return this;
    }

    enter(domNode, element) {
        super.enter(domNode, element);
        this._div = element.append("div");
    }

    private _prevWidth;
    private _prevHeight;
    update(domNode, element) {
        super.update(domNode, element);
        this._div
            .style("width", `${this.width()}px`)
            .style("height", `${this.height()}px`)
            ;

        ReactDOM.render(
            <ThemeProvider theme={this._theme} style={{ height: "100%" }}>{this._children}</ThemeProvider>,
            this._div.node()
        );

        //  TODO:  Hack to make command bar resize...
        if (this._prevWidth !== this.width() || this._prevHeight !== this.height()) {
            this._prevWidth = this.width();
            this._prevHeight = this.height();
            window.dispatchEvent(new Event("resize"));
        }
    }

    exit(domNode, element) {
        ReactDOM.unmountComponentAtNode(
            this._div.node()
        );
        super.enter(domNode, element);
    }

    render(callback?: (w: Widget) => void): this {
        const retVal = super.render(callback);
        return retVal;
    }
}

export interface DockPanelBase {
    title: string;
    location?: "split-top" | "split-left" | "split-right" | "split-bottom" | "tab-before" | "tab-after";
    ref?: string;
    closable?: boolean | IClosable;
}

export interface DockPanelWidget extends DockPanelBase {
    widget?: Widget;
}

export interface DockPanelComponent extends DockPanelBase {
    key: string;
    component?: JSX.Element;
}

function isDockPanelComponent(item: DockPanelWidget | DockPanelComponent): item is DockPanelComponent {
    return !!(item as DockPanelComponent).component;
}

export type DockPanelItems = (DockPanelWidget | DockPanelComponent)[];

interface DockPanelProps {
    storeID: string;
    items?: DockPanelItems
}

export const DockPanel: React.FunctionComponent<DockPanelProps> = ({
    storeID,
    items
}) => {

    const theme = useTheme();
    const [idx, setIdx] = React.useState<{ [key: string]: Widget }>({});

    const dockPanel = useConst(() => {
        const retVal = new HPCCDockPanel();
        const idx: { [key: string]: Widget } = {};
        items.forEach(item => {
            if (isDockPanelComponent(item)) {
                idx[item.key] = new ReactWidget().id(item.key);
                retVal.addWidget(idx[item.key], item.title, item.location, idx[item.ref], item.closable);
            } else if (item.widget) {
                idx[item.widget.id()] = item.widget;
                retVal.addWidget(item.widget, item.title, item.location, idx[item.ref], item.closable);
            }
        });
        setIdx(idx);
        return retVal;
    });

    const [layout, setLayout] = useUserStore(storeID, "");
    const [ready, setReady] = React.useState(false);

    React.useEffect(() => {
        if (layout !== undefined) {
            try {
                const obj = JSON.parse(layout);
                dockPanel.layout(obj);
            } catch (e) {

            }
            setReady(true);
        }
    }, [dockPanel, layout]);

    React.useEffect(() => {
        return () => {
            setLayout(JSON.stringify(dockPanel?.layout()));
        };
    }, [dockPanel, setLayout]);

    React.useEffect(() => {
        items.filter(isDockPanelComponent).forEach(item => {
            (idx[item.key] as ReactWidget)
                .theme(theme)
                .children(item.component)
                .render()
                ;
        });
    }, [idx, items, theme]);

    return <>
        {ready && <AutosizeHpccJSComponent widget={dockPanel} padding={4} debounce={false} />}
    </>;
};
