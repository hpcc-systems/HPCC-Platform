import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { HTMLWidget, Widget, Utility } from "@hpcc-js/common";
import { DockPanel as HPCCDockPanel, IClosable, WidgetAdapter } from "@hpcc-js/phosphor";
import { compare2 } from "@hpcc-js/util";
import { addPortal, removePortal, updatePortal } from "src/react/portalStore";
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

    protected _children = <div></div>;

    protected _div;

    constructor() {
        super();
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
        addPortal(this.id(), this._div.node());
    }

    update(domNode, element) {
        super.update(domNode, element);
        this._div
            .style("width", `${this.width()}px`)
            .style("height", `${this.height()}px`)
            ;
        updatePortal(this.id(), <Placeholder>{this._children}</Placeholder>);
    }

    exit(domNode, element) {
        removePortal(this.id());
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

// Extract every widget __id from a phosphor layout tree so we can validate
// whether a saved layout still matches the currently-mounted widget instances.
function extractLayoutWidgetIds(layout: any): string[] {
    if (!layout) return [];
    if (layout.type === "tab-area") {
        return (layout.widgets ?? [])
            .map((w: any) => {
                if (typeof w === "string") return w;
                return w?.__id ?? w?.id;
            })
            .filter((id: any): id is string => typeof id === "string" && id.length > 0);
    }
    if (layout.type === "split-area") {
        return (layout.children ?? []).flatMap(extractLayoutWidgetIds);
    }
    if (layout.main) {
        return extractLayoutWidgetIds(layout.main);
    }
    return [];
}

export class ResetableDockPanel extends HPCCDockPanel {

    protected _origLayout: DockPanelLayout | undefined;
    protected _lastLayout: DockPanelLayout | undefined;
    protected _visibility: { [id: string]: boolean };
    protected _disposed = false;

    markDisposed(disposed: boolean = true) {
        this._disposed = disposed;
    }

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
        // Only restore if every saved widget ID exists in the current dock panel.
        // Stale IDs (from a previous mount or session) cause phosphor to create
        // empty dummy WidgetAdapters that replace the real ReactWidget adapters,
        // leaving every panel blank after back-navigation.
        const currentIds = new Set(this.widgets().map(w => w.id()));
        const savedIds = extractLayoutWidgetIds(layout);
        if (savedIds.length > 0 && savedIds.every(id => currentIds.has(id))) {
            this.layout(layout);
        }
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
        if (!this._disposed) {
            this.visibilityChanged(this._visibility);
        }
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
    const idx = useConst(() => new Map<string, ReactWidget>());

    // Keep refs to the latest callbacks to avoid stale closures
    const onDockPanelCreateRef = React.useRef(onDockPanelCreate);
    onDockPanelCreateRef.current = onDockPanelCreate;

    const onDockPanelVisibilityChangedRef = React.useRef(onDockPanelVisibilityChanged);
    onDockPanelVisibilityChangedRef.current = onDockPanelVisibilityChanged;

    const dockPanel = useConst(() => {
        const retVal = new ResetableDockPanel();
        retVal.on("visibilityChanged", visibility => onDockPanelVisibilityChangedRef.current?.(visibility), true);
        return retVal;
    });

    // Call onCreate once after mount; mark disposed and clean up on unmount.
    React.useEffect(() => {
        let timeoutId: ReturnType<typeof setTimeout> | undefined;
        if (onDockPanelCreateRef.current) {
            timeoutId = setTimeout(() => {
                onDockPanelCreateRef.current?.(dockPanel);
            }, 0);
        }
        return () => {
            if (timeoutId !== undefined) clearTimeout(timeoutId);
            dockPanel.markDisposed();
        };
    }, [dockPanel]);

    React.useEffect(() => {
        dockPanel?.hideSingleTabs(hideSingleTabs);
    }, [dockPanel, hideSingleTabs]);

    React.useEffect(() => {
        const diffs = compare2(prevItems, items, item => item.key);
        diffs.exit.forEach(item => {
            const reactWidget = idx.get(item.key);
            idx.delete(item.key);
            dockPanel.removeWidget(reactWidget);
        });
        diffs.enter.forEach(item => {
            const reactWidget = new ReactWidget();
            dockPanel.addWidget(reactWidget, item.props.title, item.props.location, idx.get(item.props.relativeTo), item.props.closable, item.props.padding);
            idx.set(item.key, reactWidget);
        });
        [...diffs.enter, ...diffs.update].forEach(item => {
            const reactWidget = idx.get(item.key);
            if (reactWidget) {
                reactWidget
                    .children(item.props.children)
                    ;
            }
        });
        dockPanel.render();
        setPrevItems(items);
    }, [prevItems, dockPanel, idx, items]);

    React.useEffect(() => {
        if (layout === undefined) {
            dockPanel?.resetLayout();
        } else {
            dockPanel?.setLayout(layout);
        }
    }, [dockPanel, layout]);

    return <AutosizeHpccJSComponent widget={dockPanel} padding={4} debounce={false} />;
};
