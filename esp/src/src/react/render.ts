import * as React from "react";
import { Root, createRoot } from "react-dom/client";
import { LightThemed, Themed } from "./Themed";

type Container = Element | DocumentFragment;
type Parent = Element | Document | ShadowRoot | DocumentFragment | string;

function resolveContainer(parent: Parent): Container | null {
    const resolved = typeof parent === "string" ? document.getElementById(parent) : parent;
    if (!resolved) {
        return null;
    }

    if (resolved instanceof Document) {
        const docContainer = resolved.body ?? resolved.documentElement;
        return docContainer ?? null;
    }

    if (resolved instanceof Element || resolved instanceof DocumentFragment) {
        return resolved;
    }

    return null;
}

export class ReactRoot {
    protected _root?: Root;

    protected constructor(root: Root) {
        this._root = root;
    }

    static create(parent: Parent): ReactRoot {
        const container = resolveContainer(parent);
        if (!container) {
            throw new Error("Target container is not a DOM element.");
        }
        return new ReactRoot(createRoot(container));
    }

    dispose(): void {
        this._root?.unmount();
        this._root = undefined;
    }

    render<P>(C: React.FunctionComponent<P>, props: Readonly<P>) {
        if (!this._root) {
            throw new Error("Component has been unmounted.");
        }
        this._root.render(React.createElement(C, props));
    }

    themedRender<P>(C: React.FunctionComponent<P>, props: Readonly<P>) {
        if (!this._root) {
            throw new Error("Component has been unmounted.");
        }
        this._root.render(React.createElement(Themed, null, React.createElement(C, props)));
    }

    lightThemedRender<P>(C: React.FunctionComponent<P>, props: Readonly<P>) {
        if (!this._root) {
            throw new Error("Component has been unmounted.");
        }
        this._root.render(React.createElement(LightThemed, null, React.createElement(C, props)));
    }

    svgRender<P>(C: React.FunctionComponent<P>, props: Readonly<P>) {
        if (!this._root) {
            throw new Error("Component has been unmounted.");
        }
        this._root.render(React.createElement("svg", null, React.createElement(C, props)));
    }
}

