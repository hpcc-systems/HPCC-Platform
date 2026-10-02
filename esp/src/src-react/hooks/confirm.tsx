import * as React from "react";
import { Button, Dialog, DialogActions, DialogBody, DialogContent, DialogOpenChangeData, DialogOpenChangeEvent, DialogSurface, DialogTitle, Field, Input, makeStyles } from "@fluentui/react-components";
import nlsHPCC from "src/nlsHPCC";

const useStyles = makeStyles({
    surface: {
        maxWidth: "500px",
    },
});

export interface ConfirmField {
    id: string;
    label: string;
    value?: string;
}

interface useConfirmProps {
    title: string;
    message: string;
    items?: string[];
    fields?: ConfirmField[];
    submitLabel?: string;
    cancelLabel?: string;
    onSubmit: (values?: Record<string, string>) => void;
}

export function useConfirm({ title, message, items = [], fields = [], onSubmit, submitLabel = nlsHPCC.OK, cancelLabel = nlsHPCC.Cancel }: useConfirmProps): [React.FunctionComponent, (_: boolean) => void] {

    const styles = useStyles();
    const [show, setShow] = React.useState(false);
    const [fieldValues, setFieldValues] = React.useState<Record<string, string>>(() =>
        Object.fromEntries(fields.map(f => [f.id, f.value ?? ""]))
    );

    // reading everything through this ref avoids React remounting the Dialog while typing into fields
    const latest = React.useRef({ title, message, items, fields, submitLabel, cancelLabel, onSubmit, show, fieldValues });
    latest.current = { title, message, items, fields, submitLabel, cancelLabel, onSubmit, show, fieldValues };

    const setShowExternal = React.useCallback((visible: boolean) => {
        if (visible) {
            setFieldValues(Object.fromEntries(latest.current.fields.map(f => [f.id, f.value ?? ""])));
        }
        setShow(visible);
    }, []);

    const Confirm = React.useMemo(() => () => {
        const { title, message, items, fields, submitLabel, cancelLabel, onSubmit, show, fieldValues } = latest.current;
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
                        {fields.map(field => {
                            return <Field key={field.id} label={field.label}>
                                <Input
                                    value={fieldValues[field.id] ?? ""}
                                    onChange={(_, data) => setFieldValues(prev => ({ ...prev, [field.id]: data.value }))}
                                />
                            </Field>;
                        })}
                    </DialogContent>
                    <DialogActions>
                        <Button appearance="primary" onClick={() => { if (typeof onSubmit === "function") { onSubmit(latest.current.fieldValues); } setShow(false); }}>{submitLabel}</Button>
                        {cancelLabel && <Button onClick={() => setShow(false)}>{cancelLabel}</Button>}
                    </DialogActions>
                </DialogBody>
            </DialogSurface>
        </Dialog>;
    }, [styles.surface]);

    return [Confirm, setShowExternal];
}

