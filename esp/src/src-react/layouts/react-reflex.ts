import { makeStyles } from "@fluentui/react-components";
export { ReflexContainer, ReflexElement, ReflexSplitter } from "react-reflex";

import "react-reflex/styles.css";

export const useReflexClassNames = makeStyles({
    reflexScrollPane: {
        borderTopWidth: "1px",
        borderRightWidth: "1px",
        borderBottomWidth: "1px",
        borderLeftWidth: "1px",
        borderTopStyle: "solid",
        borderRightStyle: "solid",
        borderBottomStyle: "solid",
        borderLeftStyle: "solid",
        borderTopColor: "darkgray",
        borderRightColor: "darkgray",
        borderBottomColor: "darkgray",
        borderLeftColor: "darkgray",
    },
    reflexPane: {
        borderTopWidth: "1px",
        borderRightWidth: "1px",
        borderBottomWidth: "1px",
        borderLeftWidth: "1px",
        borderTopStyle: "solid",
        borderRightStyle: "solid",
        borderBottomStyle: "solid",
        borderLeftStyle: "solid",
        borderTopColor: "darkgray",
        borderRightColor: "darkgray",
        borderBottomColor: "darkgray",
        borderLeftColor: "darkgray",
        overflow: "hidden"
    },
    reflexSplitter: {
        position: "relative",
        height: "5px",
        backgroundColor: "transparent",
        borderTopStyle: "none",
        borderRightStyle: "none",
        borderBottomStyle: "none",
        borderLeftStyle: "none",
    },
    reflexSplitterDiv: {
        fontFamily: "Lucida Sans,Lucida Grande,Arial",
        fontSize: "13px",
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
