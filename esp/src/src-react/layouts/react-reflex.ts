import { mergeStyleSets } from "@fluentui/react";
export { ReflexContainer, ReflexElement, ReflexSplitter } from "react-reflex";

import "react-reflex/styles.css";

export const classNames = mergeStyleSets({
    reflexScrollPane: {
        borderWidth: 1,
        borderStyle: "solid",
        borderColor: "darkgray"
    },
    reflexPane: {
        borderWidth: 1,
        borderStyle: "solid",
        borderColor: "darkgray",
        overflow: "hidden"
    },
    reflexSplitter: {
        position: "relative",
        height: "5px",
        backgroundColor: "transparent",
        borderStyle: "none"
    },
    reflexSplitterDiv: {
        fontFamily: "Lucida Sans,Lucida Grande,Arial !important",
        fontSize: "13px !important",
        cursor: "row-resize",
        position: "absolute",
        left: "49%",
        background: "#9e9e9e",
        height: "1px",
        top: "2px",
        width: "19px"
    }
});

export const styles = {
    reflexSplitter: { height: 5, position: "relative" } as React.CSSProperties
};
