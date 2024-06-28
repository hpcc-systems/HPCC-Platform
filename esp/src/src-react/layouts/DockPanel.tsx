import * as React from "react";
import * as ReactDOM from "react-dom";
import { Theme, ThemeProvider } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { FluentProvider, Theme as ThemeV9 } from "@fluentui/react-components";
import { HTMLWidget, Widget } from "@hpcc-js/common";
import { DockPanel as HPCCDockPanel, IClosable } from "@hpcc-js/phosphor";
import { compare2 } from "@hpcc-js/util";
import { lightTheme, lightThemeV9 } from "../themes";
import { useUserTheme } from "../hooks/theme";
import { AutosizeHpccJSComponent } from "./HpccJSAdapter";

export class ReactWidget extends HTMLWidget {

    protected _theme: Theme = lightTheme;
    protected _themeV9: ThemeV9 = lightThemeV9;
    protected _children = <div></div>;

    protected _div;

    constructor() {
        super();
    }

    theme(): Theme;
    theme(_: Theme): this;
    theme(_?: Theme): this | Theme {
        if (arguments.length === 0) return this._theme;
        this._theme = _;
        return this;
    }

    themeV9(): ThemeV9;
    themeV9(_: ThemeV9): this;
    themeV9(_?: ThemeV9): this | ThemeV9 {
        if (arguments.length === 0) return this._themeV9;
        this._themeV9 = _;
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

    update(domNode, element) {
        super.update(domNode, element);
        this._div
            .style("width", `${this.width()}px`)
            .style("height", `${this.height()}px`)
            ;

        ReactDOM.render(
            <FluentProvider theme={this._themeV9} style={{ height: "100%" }}>
                <ThemeProvider theme={this._theme} style={{ height: "100%" }}>{this._children}</ThemeProvider>
            </FluentProvider>,
            this._div.node()
        );
    }

    exit(domNode, element) {
        setTimeout(() => ReactDOM.unmountComponentAtNode(this._div.node()), 0);
        super.enter(domNode, element);
    }

    render(callback?: (w: Widget) => void): this {
        const retVal = super.render(callback);
        return retVal;
    }
}

export interface DockPanelLayout {
    main: object;
}

function validLayout(layout?: any) {
    return !!layout?.main;
}

function formatLayout(layout?: any): DockPanelLayout | undefined {
    if (validLayout(layout)) {
        return layout;
    }
    return undefined;
}

export class ResetableDockPanel extends HPCCDockPanel {

    protected _origLayout: DockPanelLayout | undefined;
    protected _lastLayout: DockPanelLayout | undefined;

    resetLayout() {
        if (this._origLayout) {
            this
                .layout(this._origLayout)
                .lazyRender()
                ;
        }
    }

    setLayout(layout: object) {
        if (this._origLayout === undefined) {
            this._origLayout = formatLayout(this.layout());
        }
        this.layout(layout);
        return this;
    }

    getLayout() {
        return formatLayout(this.layout()) ?? this._lastLayout ?? this._origLayout;
    }

    render(callback?: (w: Widget) => void) {
        const retVal = super.render();
        if (this._origLayout === undefined) {
            this._origLayout = formatLayout(this.layout());
        }
        if (callback) {
            callback(this);
        }
        return retVal;
    }

    //  Events  ---
    layoutChanged() {
        this._lastLayout = this.getLayout();
    }
}

interface DockPanelItemProps {
    key: string;
    title: string;
    location?: "split-top" | "split-left" | "split-right" | "split-bottom" | "tab-before" | "tab-after";
    relativeTo?: string;
    closable?: boolean | IClosable;
    padding?: number;
    children: JSX.Element;
}

export const DockPanelItem: React.FunctionComponent<DockPanelItemProps> = ({
    children
}) => {
    return <>{children}</>;
};

interface DockPanelProps {
    layout?: object;
    hideSingleTabs?: boolean;
    onDockPanelCreate?: (dockpanel: ResetableDockPanel) => void;
    children?: React.ReactElement<DockPanelItemProps> | React.ReactElement<DockPanelItemProps>[];
}

export const DockPanel: React.FunctionComponent<DockPanelProps> = ({
    layout,
    hideSingleTabs,
    onDockPanelCreate,
    children
}) => {
    const items = React.useMemo(() => {
        if (children === undefined) return [];
        return (Array.isArray(children) ? children : [children]).filter(item => !!item);
    }, [children]);
    const [prevItems, setPrevItems] = React.useState<React.ReactElement<DockPanelItemProps>[]>([]);
    const { theme, themeV9 } = useUserTheme();
    const idx = useConst(() => new Map<string, ReactWidget>());

    const dockPanel = useConst(() => {
        const retVal = new ResetableDockPanel();
        if (onDockPanelCreate) {
            setTimeout(() => {
                onDockPanelCreate(retVal);
            }, 0);
        }
        return retVal;
    });

    React.useEffect(() => {
        dockPanel?.hideSingleTabs(hideSingleTabs);
    }, [dockPanel, hideSingleTabs]);

    React.useEffect(() => {
        const diffs = compare2(prevItems, items, item => item.key);
        diffs.exit.forEach(item => {
            idx.delete(item.key);
            dockPanel.removeWidget(idx.get(item.key));
        });
        diffs.enter.forEach(item => {
            const reactWidget = new ReactWidget()
                .id(item.key)
                ;
            dockPanel.addWidget(reactWidget, item.props.title, item.props.location, idx.get(item.props.relativeTo), item.props.closable, item.props.padding);
            idx.set(item.key, reactWidget);
        });
        [...diffs.enter, ...diffs.update].forEach(item => {
            const reactWidget = idx.get(item.key);
            if (reactWidget) {
                reactWidget
                    .theme(theme)
                    .themeV9(themeV9)
                    .children(item.props.children)
                    ;
            }
        });
        dockPanel.render();
        setPrevItems(items);
    }, [prevItems, dockPanel, idx, items, theme, themeV9]);

    React.useEffect(() => {
        if (layout === undefined) {
            dockPanel?.resetLayout();
        } else {
            dockPanel?.setLayout(layout);
        }
    }, [dockPanel, layout]);

    return <AutosizeHpccJSComponent widget={dockPanel} padding={4} debounce={false} />;
};
