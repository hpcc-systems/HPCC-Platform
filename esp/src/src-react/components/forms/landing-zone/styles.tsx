import { makeStyles, tokens } from "@fluentui/react-components";

export const useComponentStyles = makeStyles({
    container: {
        display: "flex",
        flexFlow: "column nowrap",
        alignItems: "stretch",
    },
    header: {
        flex: "1 1 auto",
        borderTop: `4px solid ${tokens.colorBrandBackground}`,
        display: "flex",
        alignItems: "center",
        fontWeight: tokens.fontWeightSemibold,
        padding: "12px 12px 14px 24px",
    },
    body: {
        flex: "4 4 auto",
        padding: "0 24px 24px 24px",
        overflowY: "hidden",
        "& p": { margin: "14px 0" },
        "& p:first-child": { marginTop: 0 },
        "& p:last-child": { marginBottom: 0 },
    },
    selectionTable: {
        padding: "4px",
        border: `1px solid ${tokens.colorBrandBackground2}`
    },
    twoColumnTable: {
        marginTop: "14px",
        "& tr": { marginTop: "10px" }
    }
});