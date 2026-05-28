import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { daliService } from "../comms/dali";
import { useDaliResult } from "../hooks/useDaliResult";
import { useConfirm } from "../hooks/confirm";
import { DaliAdminForm } from "./DaliAdminForm";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/DaliDelete.tsx");

export const DaliDelete: React.FunctionComponent = () => {
    const { columns, items, setResult } = useDaliResult();
    const [path, setPath] = React.useState("");

    const [ConfirmDialog, confirmAction] = useConfirm({
        title: nlsHPCC.DaliAdmin,
        message: nlsHPCC.DaliPromptConfirm,
        onSubmit: React.useCallback(() => {
            daliService.Delete({ Path: path })
                .then(response => setResult(response.Result))
                .catch(err => logger.error(err));
        }, [path, setResult]),
    });

    const onSubmit = React.useCallback(() => confirmAction(true), [confirmAction]);

    return <DaliAdminForm
        fields={{ "Path": { label: nlsHPCC.Path, type: "string", value: path } }}
        onChange={(_, v) => setPath(v)}
        onSubmit={onSubmit}
        columns={columns}
        items={items}
        confirmDialog={<ConfirmDialog />}
    />;
};
