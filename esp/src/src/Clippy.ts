import * as Clipboard from "clipboard";
import * as Tooltip from "dijit/Tooltip";
import * as dom from "dojo/dom";
import * as mouse from "dojo/mouse";
import * as on from "dojo/on";
import nlsHPCC from "./nlsHPCC";

export function attach(domID: string): void {
    const clipboard: Clipboard = new Clipboard(`#${domID}`);
    clipboard.on("success", e => {
        e.clearSelection();
        const node: HTMLElement = dom.byId(domID);
        Tooltip.show(nlsHPCC.Copied, node);
        on.once(node, mouse.leave, () => {
            Tooltip.hide(node);
        });
    });

    clipboard.on("error", e => {
        const node: HTMLElement = dom.byId(domID);
        Tooltip.show(nlsHPCC.PressCtrlCToCopy, node);
        on.once(node, mouse.leave, () => {
            Tooltip.hide(node);
        });
    });
}

export function attachDomNode(domNode: HTMLElement, callback: () => string): void {
    const clipboard = new Clipboard(domNode, {
        text: trigger => callback()
    });

    clipboard.on("success", e => {
        Tooltip.show(nlsHPCC.Copied, domNode);
        on.once(domNode, mouse.leave, () => {
            Tooltip.hide(domNode);
        });
    });

    clipboard.on("error", e => {
    });
}
