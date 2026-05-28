import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { daliService } from "../comms/dali";
import { useDaliResult } from "../hooks/useDaliResult";
import { useFormFields } from "../hooks/useFormFields";
import { DaliAdminForm } from "./DaliAdminForm";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/GetLogicalFilePart.tsx");

export const GetLogicalFilePart: React.FunctionComponent = () => {
    const { columns, items, setResult } = useDaliResult();
    const [fields, handleChange] = useFormFields({ FileName: "", PartNumber: 1 });

    const onSubmit = React.useCallback(() => {
        daliService.GetLogicalFilePart({ FileName: fields.FileName, PartNumber: fields.PartNumber })
            .then(response => setResult(response.Result))
            .catch(err => logger.error(err));
    }, [fields, setResult]);

    return <DaliAdminForm
        fields={{
            "FileName": { label: nlsHPCC.FileName, type: "string", value: fields.FileName },
            "PartNumber": { label: nlsHPCC.PartNumber, type: "number", value: fields.PartNumber },
        }}
        onChange={handleChange}
        onSubmit={onSubmit}
        columns={columns}
        items={items}
    />;
};
