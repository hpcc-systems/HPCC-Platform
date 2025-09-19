import * as React from "react";
import { Dialog, DialogSurface, DialogBody, DialogTitle, DialogContent, DialogActions, makeStyles } from "@fluentui/react-components";

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
    undraggable?: boolean;
}

export const MessageBox: React.FunctionComponent<MessageBoxProps> = ({
    title,
    minWidth = 360,
    show,
    onDismiss,
    setShow,
    footer,
    children,
    disableClose = false,
    undraggable = false
}) => {

    const styles = useStyles();

    const close = React.useCallback(() => {
        if (disableClose) return;
        if (onDismiss) {
            onDismiss();
        }
        setShow(false);
    }, [disableClose, onDismiss, setShow]);

    const surfaceRef = React.useRef<HTMLDivElement | null>(null);
    const positionRef = React.useRef({ x: 0, y: 0 });
    const sizeRef = React.useRef({ width: 0, height: 0 });

    interface DragState {
        dragging: boolean;
        startX: number;
        startY: number;
        origX: number;
        origY: number;
    }
    const dragRef = React.useRef<DragState>({ dragging: false, startX: 0, startY: 0, origX: 0, origY: 0 });

    const onMouseMove = React.useCallback((evt: MouseEvent) => {
        if (!dragRef.current.dragging || !surfaceRef.current) {
            return;
        }

        const dialogSurface = surfaceRef.current;

        if (sizeRef.current.width === 0 || sizeRef.current.height === 0) {
            const rect = dialogSurface.getBoundingClientRect();
            sizeRef.current = { width: rect.width, height: rect.height };
        }

        const maxX = window.innerWidth - sizeRef.current.width;
        const maxY = window.innerHeight - sizeRef.current.height;

        let newX = dragRef.current.origX + (evt.clientX - dragRef.current.startX);
        let newY = dragRef.current.origY + (evt.clientY - dragRef.current.startY);
        newX = Math.min(Math.max(0, newX), Math.max(0, maxX));
        newY = Math.min(Math.max(0, newY), Math.max(0, maxY));

        positionRef.current = { x: newX, y: newY };
        dialogSurface.style.left = `${newX}px`;
        dialogSurface.style.top = `${newY}px`;
    }, []);

    const endDrag = React.useCallback(() => {
        window.removeEventListener("mousemove", onMouseMove);
        window.removeEventListener("mouseup", endDrag);

        if (dragRef.current.dragging) {
            dragRef.current.dragging = false;
        }

        document.body.style.userSelect = "";
    }, [onMouseMove]);

    const startDrag = React.useCallback((evt: React.MouseEvent) => {
        evt.preventDefault();

        if (undraggable || evt.button !== 0 || !surfaceRef.current) {
            return;
        }

        const dialogSurface = surfaceRef.current;
        dialogSurface.style.position = "fixed";
        dialogSurface.style.margin = "0";
        dialogSurface.style.transform = "none";

        const rect = dialogSurface.getBoundingClientRect();
        sizeRef.current = { width: rect.width, height: rect.height };

        if (positionRef.current.x === 0 && positionRef.current.y === 0) {
            positionRef.current = { x: rect.left, y: rect.top };
        }

        dragRef.current = {
            dragging: true,
            startX: evt.clientX,
            startY: evt.clientY,
            origX: positionRef.current.x,
            origY: positionRef.current.y
        };

        document.body.style.userSelect = "none";
        window.addEventListener("mousemove", onMouseMove);
        window.addEventListener("mouseup", endDrag);
    }, [undraggable, endDrag, onMouseMove]);

    // need to manually position initially when draggable, avoids a weird jitter on drag start
    React.useEffect(() => {
        if (show && !undraggable && surfaceRef.current) {
            window.requestAnimationFrame(() => {
                if (!surfaceRef.current) return;

                const dialogSurface = surfaceRef.current;
                const rect = dialogSurface.getBoundingClientRect();
                sizeRef.current = { width: rect.width, height: rect.height };
                const x = Math.max(0, (window.innerWidth - rect.width) / 2);
                const y = Math.max(0, (window.innerHeight - rect.height) / 2 - 48);

                positionRef.current = { x, y };
                dialogSurface.style.position = "fixed";
                dialogSurface.style.top = `${y}px`;
                dialogSurface.style.left = `${x}px`;
                dialogSurface.style.margin = "0";
                dialogSurface.style.transform = "none";
            });
        }
        if (!show && dragRef.current.dragging) {
            endDrag();
        }
    }, [show, undraggable, endDrag]);

    React.useEffect(() => {
        return () => endDrag();
    }, [endDrag]);

    return <Dialog open={show} modalType="non-modal" onOpenChange={(_, data) => { if (!data.open) close(); }}>
        <DialogSurface ref={surfaceRef} className={styles.surface} style={{ minWidth }}>
            <DialogBody>
                <DialogTitle onMouseDown={startDrag} style={undraggable ? undefined : { cursor: "move" }}>{title}</DialogTitle>
                <DialogContent className={styles.body}>
                    {children}
                </DialogContent>
                {footer && <DialogActions position="end">{footer}</DialogActions>}
            </DialogBody>
        </DialogSurface>
    </Dialog>;
};
