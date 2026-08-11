import * as React from "react";
import { Button, Dialog, DialogActions, DialogBody, DialogContent, DialogOpenChangeData, DialogOpenChangeEvent, DialogSurface, DialogTitle, makeStyles } from "@fluentui/react-components";
import { Fields } from "./Fields";
import { TableGroup } from "./Groups";
import { useFormFields } from "../../hooks/useFormFields";
import nlsHPCC from "src/nlsHPCC";

const useStyles = makeStyles({
    dialogSurface: {
        maxWidth: "600px",
        width: "100%"
    },
    content: {
        maxHeight: "65vh",
        overflowY: "auto"
    }
});

export interface FormDialogProps<T extends Record<string, any>> {
    open: boolean;
    title: string;
    initialValues: T;
    buildFields: (values: T) => Fields;
    onClose: () => void;
    onSubmit?: (values: T) => Promise<void> | void;
    submitText?: string;
    cancelText?: string;
}

export const FormDialog = <T extends Record<string, any>>({
    open,
    title,
    initialValues,
    buildFields,
    onClose,
    onSubmit,
    submitText = nlsHPCC.Submit,
    cancelText = nlsHPCC.Cancel
}: FormDialogProps<T>): React.JSX.Element => {
    const styles = useStyles();
    const [isSubmitting, setIsSubmitting] = React.useState(false);
    const [seed, setSeed] = React.useState(0);

    React.useEffect(() => {
        if (open) {
            setSeed(prev => prev + 1);
        }
    }, [open]);

    const handleOpenChange = React.useCallback((_evt: DialogOpenChangeEvent, data: DialogOpenChangeData) => {
        if (!data.open && !isSubmitting) {
            onClose();
        }
    }, [isSubmitting, onClose]);

    return <Dialog modalType="modal" open={open} onOpenChange={handleOpenChange}>
        <DialogSurface className={styles.dialogSurface}>
            <DialogBody>
                <DialogTitle>{title}</DialogTitle>
                <DialogContent className={styles.content}>
                    {open && <FormDialogContent
                        key={seed}
                        initialValues={initialValues}
                        buildFields={buildFields}
                        onClose={onClose}
                        onSubmit={onSubmit}
                        isSubmitting={isSubmitting}
                        setIsSubmitting={setIsSubmitting}
                        submitText={submitText}
                        cancelText={cancelText}
                    />}
                </DialogContent>
            </DialogBody>
        </DialogSurface>
    </Dialog>;
};

interface FormDialogContentProps<T extends Record<string, any>> {
    initialValues: T;
    buildFields: (values: T) => Fields;
    onClose: () => void;
    onSubmit?: (values: T) => Promise<void> | void;
    isSubmitting: boolean;
    setIsSubmitting: (isSubmitting: boolean) => void;
    submitText: string;
    cancelText: string;
}

const FormDialogContent = <T extends Record<string, any>>({
    initialValues,
    buildFields,
    onClose,
    onSubmit,
    isSubmitting,
    setIsSubmitting,
    submitText,
    cancelText
}: FormDialogContentProps<T>): React.JSX.Element => {
    const [fields, handleChange] = useFormFields(initialValues);

    const formFields = React.useMemo(() => buildFields(fields), [buildFields, fields]);

    const handleSubmit = React.useCallback(async () => {
        if (!onSubmit) {
            onClose();
            return;
        }
        setIsSubmitting(true);
        try {
            await onSubmit(fields);
            onClose();
        } finally {
            setIsSubmitting(false);
        }
    }, [fields, onClose, onSubmit, setIsSubmitting]);

    return <>
        <TableGroup fields={formFields} onChange={handleChange} width="100%" />
        <DialogActions>
            <Button appearance="primary" onClick={handleSubmit} disabled={isSubmitting}>{submitText}</Button>
            <Button appearance="secondary" onClick={onClose} disabled={isSubmitting}>{cancelText}</Button>
        </DialogActions>
    </>;
};
