import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { daliService } from "../comms/dali";
import { useDaliResult } from "../hooks/useDaliResult";
import { useFormFields } from "../hooks/useFormFields";
import { useConfirm } from "../hooks/confirm";
import { DaliAdminForm } from "./DaliAdminForm";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/DaliAdd.tsx");

export const DaliAdd: React.FunctionComponent = () => {
    const { columns, items, setResult } = useDaliResult();
    const [fields, handleChange] = useFormFields({ Path: "", Value: "" });

    const [ConfirmDialog, confirmAction] = useConfirm({
        title: nlsHPCC.DaliAdmin,
        message: nlsHPCC.DaliPromptConfirm,
        onSubmit: React.useCallback(() => {
            daliService.Add({ Path: fields.Path, Value: fields.Value })
                .then(response => setResult(response.Result))
                .catch(err => logger.error(err));
        }, [fields, setResult]),
    });

    const onSubmit = React.useCallback(() => confirmAction(true), [confirmAction]);

    return <DaliAdminForm
        fields={{
            "Path": { label: nlsHPCC.Path, type: "string", value: fields.Path },
            "Value": { label: nlsHPCC.Value, type: "string", value: fields.Value },
        }}
        onChange={handleChange}
        onSubmit={onSubmit}
        columns={columns}
        items={items}
        confirmDialog={<ConfirmDialog />}
    />;
};
