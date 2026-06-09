import * as React from "react";
import { Button, Field, Input, Radio, RadioGroup, Spinner } from "@fluentui/react-components";
import { useForm, Controller } from "react-hook-form";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import * as WsDfu from "src/WsDfu";
import { MessageBox } from "../../layouts/MessageBox";
import * as FormStyles from "./landing-zone/styles";

const logger = scopedLogger("src-react/components/forms/AddToSuperfile.tsx");

interface AddToSuperfileFormValues {
    superFile: string;
    names?: {
        name: string
    }[];
    existingFile: number;
}

const defaultValues: AddToSuperfileFormValues = {
    superFile: ".::superfile",
    existingFile: 0
};



interface AddToSuperfileProps {
    logicalFiles: string[];

    showForm: boolean;
    setShowForm: (_: boolean) => void;

    refreshGrid?: (_?: boolean) => void;
}

export const AddToSuperfile: React.FunctionComponent<AddToSuperfileProps> = ({
    logicalFiles,
    showForm,
    setShowForm,
    refreshGrid
}) => {

    const componentStyles = FormStyles.useComponentStyles();
    const { handleSubmit, control, reset } = useForm<AddToSuperfileFormValues>({ defaultValues });
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
                WsDfu.AddtoSuperfile(data.names, data.superFile, data.existingFile)
                    .then(response => {
                        setSubmitDisabled(false);
                        setSpinnerHidden(true);
                        closeForm();
                        if (refreshGrid) refreshGrid(true);
                    })
                    .catch(err => logger.error(err))
                    ;
            },
            err => {
                logger.debug(err);
            }
        )();
    }, [closeForm, handleSubmit, refreshGrid]);

    React.useEffect(() => {
        if (logicalFiles.length > 0) {
            const files = [];
            logicalFiles.forEach((file, idx) => {
                files.push({ "Name": file });
            });
            const newValues = { ...defaultValues, "names": files };
            reset(newValues);
        }
    }, [logicalFiles, reset]);

    return <MessageBox title={nlsHPCC.AddToSuperfile} show={showForm} setShow={closeForm}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="after" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <Button appearance="primary" disabled={submitDisabled} onClick={handleSubmit(onSubmit)}>{nlsHPCC.Add}</Button>
            <Button onClick={() => closeForm()}>{nlsHPCC.Cancel}</Button>
        </>}>
        <div style={{ display: "flex", flexDirection: "column" }}>
            <Controller
                control={control} name="superFile"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <Field label={nlsHPCC.SuperFile} validationMessage={error?.message}>
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
                control={control} name="existingFile"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <RadioGroup
                    name={fieldName}
                    onChange={(_evt, data) => onChange(Number(data.value))}
                    defaultValue="0"
                >
                        <Radio value="0" label={nlsHPCC.CreateANewFile} />
                        <Radio value="1" label={nlsHPCC.AddToExistingSuperfile} />
                    </RadioGroup>}
            />
            {logicalFiles?.length > 0 &&
                <div style={{ display: "flex", flexDirection: "column" }}>
                    <table className={`${componentStyles.twoColumnTable} ${componentStyles.selectionTable}`}>
                        <thead>
                            <tr>
                                <th>{nlsHPCC.LogicalName}</th>
                            </tr>
                        </thead>
                        <tbody>
                            {logicalFiles.map((file, idx) => {
                                return <tr key={`File-${idx}`}>
                                    <td>
                                        <Controller
                                            control={control} name={`names.${idx}.name` as const}
                                            render={({
                                                field: { onChange, name: fieldName },
                                                fieldState: { error }
                                            }) => <Field validationMessage={error?.message}>
                                                    <Input
                                                        name={fieldName}
                                                        value={file}
                                                        readOnly
                                                        onChange={(_, data) => onChange(data.value)}
                                                    />
                                                </Field>}
                                        />
                                    </td>
                                </tr>;
                            })}
                        </tbody>
                    </table>
                </div>
            }
        </div>
    </MessageBox>;
};
