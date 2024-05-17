import { select } from "@hpcc-js/common";
import { ECLEditorWidget, IconBarWidget } from "./ECLEditorPlay";

function onPageLoad() {
    select("body").selectAll("pre.programlisting:not([lang]),pre.programlisting[lang='ecl'],pre.programlisting[lang='ecl_runnable']")
        .each(function (d, i) {
            const element = this as HTMLPreElement;
            const bbox = element.getBoundingClientRect();

            const root = document.createElement("div");
            root.style.position = "relative";
            root.style.marginLeft = "3.25em";

            element.parentNode!.insertBefore(root, element);

            const toolbarDiv = document.createElement("div");
            toolbarDiv.style.position = "absolute";
            toolbarDiv.style.top = "0";
            toolbarDiv.style.left = "0";
            toolbarDiv.style.width = "100%";
            toolbarDiv.style.height = "100%";
            root.appendChild(toolbarDiv);

            const editorDiv = document.createElement("div");
            editorDiv.style.top = "0";
            editorDiv.style.left = "0";
            editorDiv.style.width = "100%";
            root.appendChild(editorDiv);

            const d3Element = select(this);

            const iconBar = new IconBarWidget(d3Element.text().trim(), element.lang === "ECL_Runnable")
                .target(toolbarDiv)
                .render()
                ;

            const cm = new ECLEditorWidget()
                .target(editorDiv)
                .resize({ width: Math.max(800, bbox.width), height: bbox.height })
                .text(d3Element.text().trim())
                .readOnly(true)
                .render()
                ;

            d3Element.remove();

            iconBar?.resize()?.lazyRender();
            cm.resize().lazyRender();

            window.addEventListener("resize", function () {
                iconBar?.resize()?.lazyRender();
                cm.resize().lazyRender();
            });
        });
}

window.addEventListener("DOMContentLoaded", onPageLoad);

