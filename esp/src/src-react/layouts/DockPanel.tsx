import * as React from "react";
import { Theme } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { Theme as ThemeV9 } from "@fluentui/react-components";
import { HTMLWidget, Widget, Utility } from "@hpcc-js/common";
import { DockPanel as HPCCDockPanel, IClosable, WidgetAdapter } from "@hpcc-js/phosphor";
import { compare2 } from "@hpcc-js/util";
import { ReactRoot } from "src/react/render";
import { lightTheme, lightThemeV9 } from "../themes";
import { useUserTheme } from "../hooks/theme";
import { AutosizeHpccJSComponent } from "./HpccJSAdapter";

export interface PlaceholderProps {
    children?: React.ReactNode;
}

export const Placeholder: React.FunctionComponent<PlaceholderProps> = ({
    children
}) => {
    return <>{children}</>;
};

export class ReactWidget extends HTMLWidget {

    protected _theme: Theme = lightTheme;
    protected _themeV9: ThemeV9 = lightThemeV9;
    protected _children = <div></div>;

    protected _div;
    protected _root: ReactRoot;

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

    children(): React.JSX.Element;
    children(_: React.JSX.Element): this;
    children(_?: React.JSX.Element): this | React.JSX.Element {
        if (arguments.length === 0) return this._children;
        this._children = _;
        return this;
    }

    enter(domNode, element) {
        super.enter(domNode, element);
        this._div = element.append("div");
        this._root = ReactRoot.create(this._div.node());
    }

    update(domNode, element) {
        super.update(domNode, element);
        this._div
            .style("width", `${this.width()}px`)
            .style("height", `${this.height()}px`)
            ;
        this._root?.themedRender(Placeholder, { children: this._children });
    }

    exit(domNode, element) {
        this._root?.dispose();
        super.exit(domNode, element);
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
    protected _visibility: { [id: string]: boolean };

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

    getVisibility() {
        return this._visibility;
    }

    render(callback?: (w: Widget) => void) {
        const retVal = this._visibility !== undefined ? super.render() : super.render(() => {
            if (this._visibility === undefined) {
                this._visibility = {};
                this.widgetAdapters().forEach(wa => {
                    this._visibility[wa.widget.id()] = wa.widget.visible();
                });
            }
        });
        if (this._origLayout === undefined) {
            this._origLayout = formatLayout(this.layout());
        }
        if (callback) {
            callback(this);
        }
        return retVal;
    }

    //  Events  ---
    childActivation(w: Widget, wa: WidgetAdapter) {
    }

    childVisibility(w: Widget, visible: boolean, wa: WidgetAdapter) {
        if (this._visibility && this._visibility[w.id()] !== visible) {
            this._visibility[w.id()] = visible;
            this._lazyVisibilityChanged();
        }
    }

    layoutChanged() {
        this._lastLayout = this.getLayout();
    }

    //  Exposed Events  ---
    private _lazyVisibilityChanged = Utility.debounce(async () => {
        this.visibilityChanged(this._visibility);
    }, 60);

    visibilityChanged(visibility: { [id: string]: boolean }) {
    }
}

interface DockPanelItemProps {
    key: string;
    title: string;
    location?: "split-top" | "split-left" | "split-right" | "split-bottom" | "tab-before" | "tab-after";
    relativeTo?: string;
    closable?: boolean | IClosable;
    padding?: number;
    children: React.JSX.Element;
}

export const DockPanelItem: React.FunctionComponent<DockPanelItemProps> = ({
    children
}) => {
    return <>{children}</>;
};

interface DockPanelProps {
    layout?: object;
    hideSingleTabs?: boolean;
    onCreate?: (dockpanel: ResetableDockPanel) => void;
    onVisibilityChanged?: (visibility: { [id: string]: boolean }) => void;
    children?: React.ReactElement<DockPanelItemProps> | React.ReactElement<DockPanelItemProps>[];
}

export const DockPanel: React.FunctionComponent<DockPanelProps> = ({
    layout,
    hideSingleTabs,
    onCreate: onDockPanelCreate,
    onVisibilityChanged: onDockPanelVisibilityChanged,
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
        if (onDockPanelVisibilityChanged) {
            retVal.on("visibilityChanged", visibility => onDockPanelVisibilityChanged(visibility), true);
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
