import * as React from "react";
import { Checkbox, DefaultButton, PrimaryButton, TextField, } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import { DPWorkunit } from "src/DataPatterns/DPWorkunit";
import nlsHPCC from "src/nlsHPCC";
import { MessageBox } from "../../layouts/MessageBox";
import { pushUrl } from "../../util/history";
import { TargetClusterTextField } from "./Fields";

const logger = scopedLogger("../components/forms/PublishQuery.tsx");

interface PublishFormValues {
    target: string;
    logicalFile: string;
    overwrite: boolean;
}

interface OptimizeProps {
    dpWu: DPWorkunit;
    targetCluster: string;
    logicalFile: string;

    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const Optimize: React.FunctionComponent<OptimizeProps> = ({
    dpWu,
    targetCluster,
    logicalFile,
    showForm,
    setShowForm
}) => {

    const defaultValues: PublishFormValues = React.useMemo(() => ({
        target: targetCluster,
        logicalFile: `${logicalFile}::optimized`,
        overwrite: false
    }), [logicalFile, targetCluster]);

    const { handleSubmit, control } = useForm<PublishFormValues>({ defaultValues });

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                dpWu.optimize(data.target, data.logicalFile, data.overwrite).then(wu => {
                    pushUrl(`/workunits/${wu.Wuid}`);
                }).catch(err => logger.error(err));
            },
            logger.info
        )();
    }, [dpWu, handleSubmit]);

    return <MessageBox show={showForm} setShow={closeForm} title={nlsHPCC.Optimize}
        footer={<>
            <PrimaryButton text={nlsHPCC.Optimize} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => closeForm()} />
        </>}>
        <Controller control={control} name="target" render={({
            field: { onChange, name: fieldName, value },
            fieldState: { error }
        }) => <TargetClusterTextField
                key="targetClusterField"
                label={nlsHPCC.Target}
                placeholder={nlsHPCC.Target}
                selectedKey={value}
                required={true}
                onChange={(ev, row) => {
                    return onChange(row.key);
                }}
                errorMessage={error && error?.message}
            />}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        <Controller
            control={control} name="logicalFile"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    label={nlsHPCC.Name}
                    value={value}
                    errorMessage={error && error?.message}
                    styles={{ root: { minWidth: 320 } }}
                />}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        <div style={{ paddingTop: "10px" }}>
            <Controller
                control={control} name="overwrite"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Checkbox label={nlsHPCC.Overwrite} name={fieldName} checked={value} onChange={onChange} />}
            />
        </div>
    </MessageBox>;
};