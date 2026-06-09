import * as React from "react";
import { makeStyles, tokens, Dialog, DialogSurface, DialogBody, DialogTitle, DialogContent, DialogActions, Button, DialogOpenChangeEvent, DialogOpenChangeData } from "@fluentui/react-components";
import nlsHPCC from "src/nlsHPCC";

const useStyles = makeStyles({
    dialogSurface: {
        minWidth: "480px",
        maxWidth: "640px",
    },
    content: {
        maxHeight: "60vh",
        overflowY: "auto",
        display: "flex",
        flexDirection: "column",
        gap: "4px",
    },
    row: {
        display: "grid",
        gridTemplateColumns: "1fr auto 180px",
        alignItems: "center",
        gap: "8px",
        padding: "2px 0",
    },
    swatch: {
        width: "32px",
        height: "20px",
        borderRadius: "2px",
        border: "1px solid " + tokens.colorNeutralStroke1,
        flexShrink: 0,
    },
    value: {
        fontFamily: "monospace",
        fontSize: "11px",
        color: tokens.colorNeutralForeground3,
        whiteSpace: "nowrap",
        overflow: "hidden",
        textOverflow: "ellipsis",
    },
});

const colotTokens: [string, string][] = Object.entries(tokens)
    .filter(([key]) => key.startsWith("color"))
    .sort(([a], [b]) => a.localeCompare(b)) as [string, string][];

export interface ColorTokensProps {
    show: boolean;
    onClose: () => void;
}

export const ColorTokens: React.FunctionComponent<ColorTokensProps> = ({ show, onClose }) => {

    const styles = useStyles();

    const onDialogOpenChange = React.useCallback((_: DialogOpenChangeEvent, data: DialogOpenChangeData) => {
        if (!data.open) {
            onClose();
        }
    }, [onClose]);

    return <Dialog open={show} modalType="modal" onOpenChange={onDialogOpenChange}>
        <DialogSurface className={styles.dialogSurface}>
            <DialogBody>
                <DialogTitle>{nlsHPCC.ColorTokens}</DialogTitle>
                <DialogContent>
                    <div className={styles.content}>
                        {colotTokens.map(([key, value]) => (
                            <div key={key} className={styles.row}>
                                <span>{key}</span>
                                <div className={styles.swatch} style={{ backgroundColor: value }} />
                                <span className={styles.value} title={value}>{value}</span>
                            </div>
                        ))}
                    </div>
                </DialogContent>
                <DialogActions>
                    <Button onClick={onClose} appearance="primary">{nlsHPCC.OK}</Button>
                </DialogActions>
            </DialogBody>
        </DialogSurface>
    </Dialog>;
};
