import * as Clipboard from "clipboard";
import * as Tooltip from "dijit/Tooltip";
import * as dom from "dojo/dom";
import * as mouse from "dojo/mouse";
import * as on from "dojo/on";

export function attach(domID: string): void {
    const clipboard: Clipboard = new Clipboard(`#${domID}`);
    clipboard.on("success", e => {
        e.clearSelection();
        const node: HTMLElement = dom.byId(domID);
        Tooltip.show("Copied!", node);
        on.once(node, mouse.leave, () => {
            Tooltip.hide(node);
        });
    });

    clipboard.on("error", e => {
        const node: HTMLElement = dom.byId(domID);
        Tooltip.show("Press ctrl+c to copy.", node);
        on.once(node, mouse.leave, () => {
            Tooltip.hide(node);
        });
    });
}
