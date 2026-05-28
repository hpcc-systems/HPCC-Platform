import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { daliService } from "../comms/dali";
import { useDaliResult } from "../hooks/useDaliResult";
import { useFormFields } from "../hooks/useFormFields";
import { DaliAdminForm } from "./DaliAdminForm";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/GetProtectedList.tsx");

export const GetProtectedList: React.FunctionComponent = () => {
    const { columns, items, setResult } = useDaliResult();
    const [fields, handleChange] = useFormFields({ FileName: "", CallerId: "" });

    const onSubmit = React.useCallback(() => {
        daliService.GetProtectedList({ FileName: fields.FileName, CallerId: fields.CallerId })
            .then(response => setResult(response.Result))
            .catch(err => logger.error(err));
    }, [fields, setResult]);

    return <DaliAdminForm
        fields={{
            "FileName": { label: nlsHPCC.FileName, type: "string", value: fields.FileName },
            "CallerId": { label: nlsHPCC.CallerID, type: "string", value: fields.CallerId },
        }}
        onChange={handleChange}
        onSubmit={onSubmit}
        columns={columns}
        items={items}
    />;
};
