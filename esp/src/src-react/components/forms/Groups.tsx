import * as React from "react";
import { Label } from "@fluentui/react";
import { createInputs, Fields } from "./Fields";

interface FieldsTableProps {
    fields: Fields;
    onChange?: (id: string, newValue: any) => void;
}

export const TableGroup: React.FunctionComponent<FieldsTableProps> = ({
    fields,
    onChange = (id: string, newValue: any) => { }
}) => {

    const formFields: { id: string, label: string, field: any }[] = createInputs(fields, onChange);

    return <table style={{ padding: 4 }}>
        <tbody>
            {formFields.map((ff) => {
                return <tr key={ff.id}>
                    <td style={{ whiteSpace: "nowrap" }}><Label htmlFor={ff.id}>{ff.label}</Label></td>
                    <td style={{ width: "80%", paddingLeft: 8 }}>{ff.field}</td>
                </tr>;
            })}
        </tbody>
    </table>;
};

export const SimpleGroup: React.FunctionComponent<FieldsTableProps> = ({
    fields,
    onChange = (id: string, newValue: any) => { }
}) => {

    const formFields: { id: string, label: string, field: any }[] = createInputs(fields, onChange);

    return <>
        {
            formFields.map((ff) => {
                return <>
                    <Label htmlFor={ff.id}>{ff.label}</Label>
                    {ff.field}
                </>;
            })
        }
    </>;
};
