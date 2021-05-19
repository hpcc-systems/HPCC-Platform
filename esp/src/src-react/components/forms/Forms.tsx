import * as React from "react";
import { TableGroup } from "./Groups";
import { Fields, Values } from "./Fields";

const fieldsToRequest = (fields: Fields) => {
    const retVal: Values = {};
    for (const name in fields) {
        const field = fields[name];
        if (!field.disabled(fields)) {
            switch (field.type) {
                case "links":
                    break;
                default:
                    retVal[name] = field.value;
            }
        }
    }
    return retVal;
};

interface TableFormProps {
    fields: Fields;
    doSubmit: boolean;
    doReset: boolean;
    onSubmit: (fields: Values) => void;
    onReset: (fields: Values) => void;
}

export const TableForm: React.FunctionComponent<TableFormProps> = ({
    fields,
    doSubmit,
    doReset,
    onSubmit,
    onReset
}) => {

    const [localFields, setLocalFields] = React.useState<Fields>({ ...fields });

    React.useEffect(() => {
        if (doSubmit === false) return;
        onSubmit(fieldsToRequest(localFields));
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [doSubmit]);

    React.useEffect(() => {
        if (doReset === false) return;
        for (const key in localFields) {
            const field = localFields[key];
            switch (field.type) {
                case "links":
                    break;
                default:
                    delete field.value;
            }
        }
        setLocalFields(localFields);
        onReset(fieldsToRequest(localFields));
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [doReset]);

    return <TableGroup fields={localFields} onChange={(id, value) => {
        const field = localFields[id];
        switch (field.type) {
            case "links":
                break;
            default:
                field.value = value;
        }
        setLocalFields({ ...localFields });
    }} />;
};

