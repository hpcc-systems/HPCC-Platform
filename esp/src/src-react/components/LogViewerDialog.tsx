import * as React from "react";
import { makeStyles, Dialog, Button, DialogSurface, DialogBody, DialogTitle, DialogContent, DialogActions, DialogOpenChangeEvent, DialogOpenChangeData } from "@fluentui/react-components";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { LogViewer, defaultSort } from "./LogViewer";

const useStyles = makeStyles({
    dialogSurface: {
        maxWidth: "90vw",
        maxHeight: "90vh",
        width: "1200px",
        height: "800px",
    },
    dialogContent: {
        display: "flex",
        flexDirection: "column",
        flex: "1",
        overflow: "hidden",
        padding: "0",
        height: "600px",
    },
});

interface LogViewerDialogProps {
    show?: boolean;
    onClose?: () => void;
    sort?: QuerySortItem;
}

export const LogViewerDialog: React.FunctionComponent<LogViewerDialogProps> = ({
    show = false,
    onClose = () => { },
    sort = defaultSort
}) => {

    const styles = useStyles();

    const onDialogOpenChange = React.useCallback((_: DialogOpenChangeEvent, data: DialogOpenChangeData) => {
        if (!data.open) {
            onClose();
        }
    }, [onClose]);

    return <Dialog open={show} modalType="modal" onOpenChange={onDialogOpenChange}>
        <DialogSurface className={styles.dialogSurface}>
            <DialogBody>
                <DialogTitle>{nlsHPCC.ErrorWarnings}</DialogTitle>
                <DialogContent className={styles.dialogContent}>
                    <LogViewer sort={sort} />
                </DialogContent>
                <DialogActions>
                    <Button onClick={onClose} appearance="primary">{nlsHPCC.Close}</Button>
                </DialogActions>
            </DialogBody>
        </DialogSurface>
    </Dialog>;
};
