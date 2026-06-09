import * as React from "react";
import { Button, Field, Input, Spinner } from "@fluentui/react-components";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import { addUserFile } from "../../../comms/fileSpray";
import { MessageBox } from "../../../layouts/MessageBox";

const logger = scopedLogger("src-react/components/forms/AddFileForm.tsx");

interface AddFileFormValues {
    NetAddress: string;
    fullPath: string;
}

const defaultValues: AddFileFormValues = {
    NetAddress: "",
    fullPath: ""
};

interface AddFileFormProps {
    formMinWidth?: number;
    showForm: boolean;
    refreshGrid: (() => void);
    setShowForm: (_: boolean) => void;
    dropzones?: any[];
}

export const AddFileForm: React.FunctionComponent<AddFileFormProps> = ({
    formMinWidth = 300,
    showForm,
    refreshGrid,
    setShowForm,
    dropzones = []
}) => {

    const { handleSubmit, control, reset } = useForm<AddFileFormValues>({ defaultValues });
    const [submitDisabled, setSubmitDisabled] = React.useState(false);
    const [spinnerHidden, setSpinnerHidden] = React.useState(true);

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                setSubmitDisabled(true);
                setSpinnerHidden(false);

                // find dropzone by NetAddress
                const dropZone = dropzones.find(dz =>
                    dz.TpMachines?.TpMachine?.some(machine =>
                        machine.Netaddress === data.NetAddress ||
                        machine.ConfigNetaddress === data.NetAddress
                    )
                );

                if (!dropZone) {
                    logger.error(`Dropzone not found for NetAddress: ${data.NetAddress}`);
                    setSubmitDisabled(false);
                    setSpinnerHidden(true);
                    return;
                }

                addUserFile({
                    NetAddress: data.NetAddress,
                    fullPath: data.fullPath,
                    dropZone: dropZone
                }).then((fileItem) => {
                    logger.debug(`Successfully added user file: ${fileItem.name}`);
                    refreshGrid();
                    setSubmitDisabled(false);
                    setSpinnerHidden(true);
                    closeForm();
                    reset(defaultValues);
                }).catch((error) => {
                    logger.error(error);
                    setSubmitDisabled(false);
                    setSpinnerHidden(true);
                });
            },
            err => {
                logger.error(err);
                setSubmitDisabled(false);
                setSpinnerHidden(true);
            }
        )();
    }, [closeForm, handleSubmit, refreshGrid, reset, dropzones]);

    return <MessageBox title={nlsHPCC.AddFile} show={showForm} setShow={closeForm}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="after" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <Button appearance="primary" disabled={submitDisabled} onClick={handleSubmit(onSubmit)}>{nlsHPCC.Add}</Button>
            <Button onClick={() => closeForm()}>{nlsHPCC.Cancel}</Button>
        </>}>
        <Controller
            control={control} name="NetAddress"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field label={nlsHPCC.IP} required validationMessage={error?.message}>
                    <Input
                        name={fieldName}
                        value={value}
                        onChange={(_, data) => onChange(data.value)}
                    />
                </Field>}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        <Controller
            control={control} name="fullPath"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field label={nlsHPCC.Path} required validationMessage={error?.message}>
                    <Input
                        name={fieldName}
                        value={value}
                        placeholder={nlsHPCC.NamePrefixPlaceholder}
                        onChange={(_, data) => onChange(data.value)}
                    />
                </Field>}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
    </MessageBox>;
};