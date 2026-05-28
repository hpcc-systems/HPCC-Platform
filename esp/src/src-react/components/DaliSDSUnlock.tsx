import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { daliService } from "../comms/dali";
import { useDaliResult } from "../hooks/useDaliResult";
import { useFormFields } from "../hooks/useFormFields";
import { useConfirm } from "../hooks/confirm";
import { DaliAdminForm } from "./DaliAdminForm";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/DaliSDSUnlock.tsx");

export const DaliSDSUnlock: React.FunctionComponent = () => {
    const { columns, items, setResult } = useDaliResult();
    const [fields, handleChange] = useFormFields({ ConnectionID: "", Close: false });

    const [ConfirmDialog, confirmAction] = useConfirm({
        title: nlsHPCC.DaliAdmin,
        message: nlsHPCC.DaliPromptConfirm,
        onSubmit: React.useCallback(() => {
            daliService.UnlockSDSLock({ ConnectionID: fields.ConnectionID, Close: fields.Close })
                .then(response => setResult(response.Result))
                .catch(err => logger.error(err));
        }, [fields, setResult]),
    });

    const onSubmit = React.useCallback(() => confirmAction(true), [confirmAction]);

    return <DaliAdminForm
        fields={{
            "ConnectionID": { label: nlsHPCC.ConnectionID, type: "string", value: fields.ConnectionID },
            "Close": { label: nlsHPCC.Close, type: "checkbox", value: fields.Close },
        }}
        onChange={handleChange}
        onSubmit={onSubmit}
        columns={columns}
        items={items}
        confirmDialog={<ConfirmDialog />}
    />;
};
