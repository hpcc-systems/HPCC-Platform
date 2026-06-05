import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { daliService } from "../comms/dali";
import { useDaliResult } from "../hooks/useDaliResult";
import { useFormFields } from "../hooks/useFormFields";
import { useConfirm } from "../hooks/confirm";
import { DaliAdminForm } from "./DaliAdminForm";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/DaliImport.tsx");

export const DaliImport: React.FunctionComponent = () => {
    const { columns, items, setResult } = useDaliResult();
    const [fields, handleChange] = useFormFields({ XML: "", Path: "", Add: false });

    const [ConfirmDialog, confirmAction] = useConfirm({
        title: nlsHPCC.DaliAdmin,
        message: nlsHPCC.DaliPromptConfirm,
        onSubmit: React.useCallback(() => {
            daliService.Import({ XML: fields.XML, Path: fields.Path, Add: fields.Add })
                .then(response => setResult(response.Result))
                .catch(err => logger.error(err));
        }, [fields, setResult]),
    });

    const onSubmit = React.useCallback(() => confirmAction(true), [confirmAction]);

    return <DaliAdminForm
        fields={{
            "XML": { label: nlsHPCC.XML, type: "string", value: fields.XML },
            "Path": { label: nlsHPCC.Path, type: "string", value: fields.Path },
            "Add": { label: nlsHPCC.Add, type: "checkbox", value: fields.Add },
        }}
        onChange={handleChange}
        onSubmit={onSubmit}
        columns={columns}
        items={items}
        confirmDialog={<ConfirmDialog />}
    />;
};
