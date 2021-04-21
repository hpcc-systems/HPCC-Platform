import * as React from "react";
import { TableGroup } from "./Groups";
import { Fields, Values } from "./Fields";

const fieldsToRequest = (fields: Fields) => {
    const retVal: Values = {};
    for (const name in fields) {
        if (!fields[name].disabled(fields)) {
            retVal[name] = fields[name].value;
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
            delete localFields[key].value;
        }
        setLocalFields(localFields);
        onReset(fieldsToRequest(localFields));
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [doReset]);

    return <TableGroup fields={localFields} onChange={(id, value) => {
        localFields[id].value = value;
        setLocalFields({ ...localFields });
    }} />;
};

