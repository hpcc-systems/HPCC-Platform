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
    }, [doSubmit, localFields, onSubmit]);

    React.useEffect(() => {
        setLocalFields({ ...fields });
    }, [fields]);

    React.useEffect(() => {
        if (doReset === false) return;

        const clearedFields: Fields = {};
        for (const key in localFields) {
            const field = localFields[key];
            if (field.type === "links") {
                clearedFields[key] = { ...field };
                continue;
            }

            const cleared = { ...field };
            switch (field.type) {
                case "dropdown":
                case "dropdown-multi":
                case "target-cluster":
                case "target-dropzone":
                case "target-server":
                case "target-group":
                case "user-groups":
                case "group-members":
                case "permission-type":
                case "esdl-esp-processes":
                case "esdl-definitions":
                case "cloud-containername":
                case "cloud-podname":
                    cleared.value = "";
                    break;
                case "checkbox":
                    cleared.value = false;
                    break;
                default:
                    cleared.value = undefined;
            }
            clearedFields[key] = cleared;
        }

        setLocalFields(clearedFields);
        onReset(fieldsToRequest(clearedFields));
    }, [doReset, localFields, onReset]);

    const onChange = React.useCallback((id, value) => {
        const field = localFields[id];
        switch (field.type) {
            case "links":
                break;
            default:
                field.value = value;
        }
        setLocalFields({ ...localFields });
    }, [localFields]);
    return <TableGroup fields={localFields} onChange={onChange} />;
};

