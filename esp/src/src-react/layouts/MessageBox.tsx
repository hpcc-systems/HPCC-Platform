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
    const [position, setPosition] = React.useState<{ x: number; y: number } | undefined>(undefined);

    const close = React.useCallback(() => {
        if (disableClose) return;
        if (onDismiss) {
            onDismiss();
        }
        setPosition(undefined);
        setShow(false);
    }, [disableClose, onDismiss, setShow]);

    const onPositionChange = React.useCallback(({ x, y }: { x: number; y: number }) => {
        setPosition({ x, y });
    }, []);

    const surfaceRef = React.useCallback((node: HTMLDivElement | null) => {
        if (node && !position) {
            const rect = node.getBoundingClientRect();
            setPosition({ x: rect.left, y: rect.top });
        }
    }, [position]);

    return <DraggableDialog open={show} modalType="non-modal" onOpenChange={(_, data) => { if (!data.open) close(); }}
        position={position} onPositionChange={onPositionChange}>
        <DraggableDialogSurface ref={surfaceRef} className={styles.surface} style={{ minWidth }}>
            <DialogBody>
                <DraggableDialogHandle tabIndex={-1}>
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
