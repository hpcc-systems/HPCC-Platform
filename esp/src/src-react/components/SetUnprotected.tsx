import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { daliService } from "../comms/dali";
import { useDaliResult } from "../hooks/useDaliResult";
import { useFormFields } from "../hooks/useFormFields";
import { useConfirm } from "../hooks/confirm";
import { DaliAdminForm } from "./DaliAdminForm";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/SetUnprotected.tsx");

export const SetUnprotected: React.FunctionComponent = () => {
    const { columns, items, setResult } = useDaliResult();
    const [fields, handleChange] = useFormFields({ FileName: "", CallerId: "" });

    const [ConfirmDialog, confirmAction] = useConfirm({
        title: nlsHPCC.DaliAdmin,
        message: nlsHPCC.DaliPromptConfirm,
        onSubmit: React.useCallback(() => {
            daliService.SetUnprotected({ FileName: fields.FileName, CallerId: fields.CallerId })
                .then(response => setResult(response.Result))
                .catch(err => logger.error(err));
        }, [fields, setResult]),
    });

    const onSubmit = React.useCallback(() => confirmAction(true), [confirmAction]);

    return <DaliAdminForm
        fields={{
            "FileName": { label: nlsHPCC.FileName, type: "string", value: fields.FileName },
            "CallerId": { label: nlsHPCC.CallerID, type: "string", value: fields.CallerId },
        }}
        onChange={handleChange}
        onSubmit={onSubmit}
        columns={columns}
        items={items}
        confirmDialog={<ConfirmDialog />}
    />;
};
