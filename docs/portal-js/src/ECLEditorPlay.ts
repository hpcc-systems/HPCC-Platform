import { Button, IconBar, Widget } from "@hpcc-js/common";
import { ECLEditor } from "@hpcc-js/codemirror";

import "./ECLEditorPlay.css";

export class IconBarWidget extends IconBar {

    constructor(text: string, runnable: boolean) {
        super();
        const buttons: Widget[] = [
            new Button().faChar("fa-copy").tooltip("Copy ECL to Clipboard")
                .on("click", () => {
                    try {
                        navigator.clipboard.writeText(text);
                    } catch (err) {
                        console.error("Failed to copy: ", err);
                    }
                })
        ];
        if (runnable) {
            buttons.unshift(
                new Button().faChar("fa-play").tooltip("Open ECL in Playground")
                    .on("click", () => {
                        window.open(`https://play.hpccsystems.com:18010/esp/files/index.html#/play?ecl=${encodeURI(text)}`, "_blank");
                    })
            );
        }

        this.buttons(buttons);
    }

    render(callback?: (w) => void): this {
        super.render(w => {
            const bbox = w.getBBox();
            w.element()
                .style("left", w.width() - bbox.width - 4)
                .style("top", 2)
                ;
            if (callback) {
                callback(w);
            }
        });
        return this;
    }
}
IconBarWidget.prototype._class += " IconBarWidget";

export class ECLEditorWidget extends ECLEditor {

    constructor() {
        super();
    }

}
ECLEditorWidget.prototype._class += " ECLEditorWidget";
