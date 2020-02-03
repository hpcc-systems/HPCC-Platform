import * as React from "react";
import * as ReactDOM from "react-dom";

export function render<P>(C: React.FunctionComponent<P>, props: Readonly<P>, parent: Element | Document | ShadowRoot | DocumentFragment, replaceNode?: Element | Text) {
    ReactDOM.render(React.createElement(C, props), parent, replaceNode);
}

export function svgRender<P>(C: React.FunctionComponent<P>, props: Readonly<P>, parent: Element | Document | ShadowRoot | DocumentFragment, replaceNode?: Element | Text) {
    ReactDOM.render(React.createElement("svg", null, React.createElement(C, props)), parent, replaceNode);
}

export function unrender(parent: Element | Document | ShadowRoot | DocumentFragment) {
    ReactDOM.unmountComponentAtNode(parent);
}
