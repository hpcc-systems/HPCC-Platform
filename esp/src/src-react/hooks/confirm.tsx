import * as React from "react";
import { Button, Dialog, DialogActions, DialogBody, DialogContent, DialogOpenChangeData, DialogOpenChangeEvent, DialogSurface, DialogTitle, makeStyles } from "@fluentui/react-components";
import nlsHPCC from "src/nlsHPCC";

const useStyles = makeStyles({
    surface: {
        maxWidth: "500px",
    },
});

interface useConfirmProps {
    title: string;
    message: string;
    items?: string[];
    submitLabel?: string;
    cancelLabel?: string;
    onSubmit: () => void;
}

export function useConfirm({ title, message, items = [], onSubmit, submitLabel = nlsHPCC.OK, cancelLabel = nlsHPCC.Cancel }: useConfirmProps): [React.FunctionComponent, (_: boolean) => void] {

    const styles = useStyles();
    const [show, setShow] = React.useState(false);

    const Confirm = React.useMemo(() => () => {
        const onOpenChange = (_: DialogOpenChangeEvent, data: DialogOpenChangeData) => {
            if (!data.open) setShow(false);
        };
        return <Dialog open={show} modalType="modal" onOpenChange={onOpenChange}>
            <DialogSurface className={styles.surface}>
                <DialogBody>
                    <DialogTitle>{title}</DialogTitle>
                    <DialogContent>
                        <p>{message}</p>
                        {items.map((item, idx) => {
                            return <span key={idx}>{item} <br /></span>;
                        })}
                    </DialogContent>
                    <DialogActions>
                        <Button appearance="primary" onClick={() => { if (typeof onSubmit === "function") { onSubmit(); } setShow(false); }}>{submitLabel}</Button>
                        {cancelLabel && <Button onClick={() => setShow(false)}>{cancelLabel}</Button>}
                    </DialogActions>
                </DialogBody>
            </DialogSurface>
        </Dialog>;
    }, [cancelLabel, items, message, onSubmit, show, styles.surface, submitLabel, title]);

    return [Confirm, setShow];
}