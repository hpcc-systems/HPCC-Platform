import { select } from "@hpcc-js/common";
import { ECLEditor } from "@hpcc-js/codemirror";

select("body").selectAll("pre.programlisting:not([lang]),pre.programlisting[lang='ecl']")
    .each(function (d, i) {
        if (this) {
            const element = this as HTMLPreElement;
            const bbox = element.getBoundingClientRect();

            const div = document.createElement("div");
            div.style.marginLeft = "3.25em";
            div.style.marginTop = "8px";
            div.style.marginBottom = "8px";
            if (element.parentNode) {
                element.parentNode.insertBefore(div, element);

                const d3Element = select(this);

                const cm = new ECLEditor()
                    .target(div)
                    .resize({ width: Math.max(800, bbox.width), height: bbox.height })
                    .text(d3Element.text().trim())
                    .readOnly(true)
                    .render()
                    ;

                d3Element.remove();

                window.addEventListener("resize", function () {
                    cm.resize().lazyRender();
                });

            }
        }
    })
    ;
