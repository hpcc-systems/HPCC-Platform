import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { daliService } from "../comms/dali";
import { useDaliResult } from "../hooks/useDaliResult";
import { useFormFields } from "../hooks/useFormFields";
import { useConfirm } from "../hooks/confirm";
import { DaliAdminForm } from "./DaliAdminForm";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/SetLogicalFilePartAttr.tsx");

export const SetLogicalFilePartAttr: React.FunctionComponent = () => {
    const { columns, items, setResult } = useDaliResult();
    const [fields, handleChange] = useFormFields({ FileName: "", PartNumber: 1, Attribute: "", Value: "" });

    const [ConfirmDialog, confirmAction] = useConfirm({
        title: nlsHPCC.DaliAdmin,
        message: nlsHPCC.DaliPromptConfirm,
        onSubmit: React.useCallback(() => {
            daliService.SetLogicalFilePartAttr({
                FileName: fields.FileName,
                PartNumber: fields.PartNumber,
                Attr: fields.Attribute,
                Value: fields.Value,
            })
                .then(response => setResult(response.Result))
                .catch(err => logger.error(err));
        }, [fields, setResult]),
    });

    const onSubmit = React.useCallback(() => confirmAction(true), [confirmAction]);

    return <DaliAdminForm
        fields={{
            "FileName": { label: nlsHPCC.FileName, type: "string", value: fields.FileName },
            "PartNumber": { label: nlsHPCC.PartNumber, type: "number", value: fields.PartNumber },
            "Attribute": { label: nlsHPCC.Attribute, type: "string", value: fields.Attribute },
            "Value": { label: nlsHPCC.Value, type: "string", value: fields.Value },
        }}
        onChange={handleChange}
        onSubmit={onSubmit}
        columns={columns}
        items={items}
        confirmDialog={<ConfirmDialog />}
    />;
};
