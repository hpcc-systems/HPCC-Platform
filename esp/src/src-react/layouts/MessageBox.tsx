import * as React from "react";
import { DialogBody, DialogTitle, DialogContent, DialogActions, makeStyles } from "@fluentui/react-components";
import { DraggableDialog, DraggableDialogSurface, DraggableDialogHandle } from "@fluentui-contrib/react-draggable-dialog";

const useStyles = makeStyles({
    surface: {
        display: "flex",
        flexDirection: "column",
        width: "fit-content",
        borderTop: "4px solid var(--colorBrandStroke1)",
    },
    body: {
        maxHeight: "570px",
        flexGrow: 1,
        overflowY: "auto"
    }
});

interface MessageBoxProps {
    title: string;
    minWidth?: number;
    show: boolean;
    onDismiss?: () => void;
    setShow: (_: boolean) => void;
    footer?: React.ReactNode;
    children?: React.ReactNode;
    disableClose?: boolean;
}

export const MessageBox: React.FunctionComponent<MessageBoxProps> = ({
    title,
    minWidth = 360,
    show,
    onDismiss,
    setShow,
    footer,
    children,
    disableClose = false
}) => {

    const styles = useStyles();

    const close = React.useCallback(() => {
        if (disableClose) return;
        if (onDismiss) {
            onDismiss();
        }
        setShow(false);
    }, [disableClose, onDismiss, setShow]);

    return <DraggableDialog open={show} modalType="non-modal" onOpenChange={(_, data) => { if (!data.open) close(); }}>
        <DraggableDialogSurface className={styles.surface} style={{ minWidth }}>
            <DialogBody>
                <DraggableDialogHandle>
                    <DialogTitle>{title}</DialogTitle>
                </DraggableDialogHandle>
                <DialogContent className={styles.body}>
                    {children}
                </DialogContent>
                {footer && <DialogActions position="end">{footer}</DialogActions>}
            </DialogBody>
        </DraggableDialogSurface>
    </DraggableDialog>;
};
