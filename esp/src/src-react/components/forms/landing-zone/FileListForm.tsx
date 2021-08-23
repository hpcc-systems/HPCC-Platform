import * as React from "react";
import { Checkbox, DefaultButton, keyframes, mergeStyleSets, PrimaryButton, Stack } from "@fluentui/react";
import { useForm, Controller } from "react-hook-form";
import { TargetDropzoneTextField, TargetFolderTextField, TargetServerTextField } from "../Fields";
import * as FileSpray from "src/FileSpray";
import nlsHPCC from "src/nlsHPCC";
import { MessageBox } from "../../../layouts/MessageBox";
import * as FormStyles from "./styles";
import { ProgressRingDotsIcon } from "@fluentui/react-icons-mdl2";

interface FileListFormValues {
    dropzone: string;
    machines: string;
    path: string;
    selectedFiles?: {
        TargetName: string,
        RecordSize: string,
        SourceFile: string,
        SourceIP: string,
    }[],
    overwrite: boolean,
}

const defaultValues: FileListFormValues = {
    dropzone: "",
    machines: "",
    path: "",
    overwrite: false,
};
interface FileListFormProps {
    formMinWidth?: number;
    showForm: boolean;
    selection: object[];
    setShowForm: (_: boolean) => void;
    onSubmit?: (_: void) => void;
}

export const FileListForm: React.FunctionComponent<FileListFormProps> = ({
    formMinWidth = 300,
    showForm,
    selection,
    setShowForm,
    onSubmit
}) => {

    const [machine, setMachine] = React.useState<string>("");
    const [directory, setDirectory] = React.useState<string>("/");
    const [pathSep, setPathSep] = React.useState<string>("/");
    const [os, setOs] = React.useState<number>();

    const [submitDisabled, setSubmitDisabled] = React.useState(false);

    const { handleSubmit, control, reset } = useForm<FileListFormValues>({ defaultValues });

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const doSubmit = React.useCallback(() => {
        const uploadFiles = (data, selection) => {
            const formData = new FormData();
            selection.forEach(file => {
                formData.append("uploadedfiles[]", file);
            });
            const uploadUrl = "/FileSpray/UploadFile.json?" +
                "upload_&rawxml_=1&NetAddress=" + machine + "&OS=" + os + "&Path=" + data.path;

            setSubmitDisabled(true);

            fetch(uploadUrl, {
                method: "POST",
                body: formData,
            })
                .then(response => response.json())
                .then(response => {
                    setSubmitDisabled(false);
                    const DFUActionResult = response?.UploadFilesResponse?.UploadFileResults?.DFUActionResult;
                    if (DFUActionResult.filter(result => result.Result !== "Success").length > 0) {
                        console.log("upload failed");
                    } else {
                        closeForm();
                        if (typeof onSubmit === "function") {
                            onSubmit();
                        }
                        reset(defaultValues);
                    }
                });
        };

        handleSubmit(
            (data, evt) => {
                if (data.overwrite) {
                    uploadFiles(data, selection);
                } else {
                    const fileNames = selection.map(file => file["name"]);
                    FileSpray.FileList({
                        request: {
                            Netaddr: machine,
                            Path: data.path
                        }
                    }).then(({ FileListResponse }) => {
                        let fileName = "";
                        FileListResponse?.files?.PhysicalFileStruct.forEach(file => {
                            if (fileNames.indexOf(file.name) > -1) {
                                fileName = file.name;
                                return;
                            }
                        });
                        if (fileName === "") {
                            uploadFiles(data, selection);
                        } else {
                            alert(nlsHPCC.OverwriteMessage + "\n" + fileNames.join("\n"));
                        }
                    });
                }

            },
            err => {
                console.log(err);
            }
        )();
    }, [closeForm, handleSubmit, machine, onSubmit, os, reset, selection]);

    const progressIconSpin = keyframes({
        "0%": {
            transform: "rotate(0deg)"
        },
        "50%": {
            transform: "rotate(180deg)"
        },
        "100%": {
            transform: "rotate(360deg)"
        }
    });

    const componentStyles = mergeStyleSets(
        FormStyles.componentStyles,
        {
            container: {
                minWidth: formMinWidth ? formMinWidth : 300,
            },
            progressMessage: {
                margin: "10px 10px 8px 0",
            },
            progressIcon: {
                animation: `${progressIconSpin} 1.55s infinite linear`
            },
        }
    );

    let setDropzone = React.useCallback((dropzone: string) => { }, []);

    return <MessageBox title={nlsHPCC.FileUploader} show={showForm} setShow={closeForm}
        footer={<>
            {submitDisabled &&
                <span className={componentStyles.progressMessage}>
                    {nlsHPCC.Uploading}... <ProgressRingDotsIcon className={componentStyles.progressIcon} />
                </span>
            }
            <PrimaryButton text={nlsHPCC.Upload} onClick={handleSubmit(doSubmit)} disabled={submitDisabled} />
            {submitDisabled &&
                <DefaultButton text={nlsHPCC.Cancel} onClick={() => closeForm()} />
            }
        </>}>
        <Stack>
            <Controller
                control={control} name="dropzone"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TargetDropzoneTextField
                        key="dropzone"
                        label={nlsHPCC.LandingZone}
                        required={true}
                        placeholder={nlsHPCC.SelectValue}
                        onChange={(evt, option) => {
                            setDirectory(option["path"] as string);
                            if (option["path"].indexOf("\\") > -1) {
                                setPathSep("\\");
                            }
                            setDropzone(option.key as string);
                            onChange(option.key);
                        }}
                        errorMessage={error && error?.message}
                    />}
                rules={{
                    required: nlsHPCC.ValidationErrorRequired
                }}
            />
            <Controller
                control={control} name="machines"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TargetServerTextField
                        key="machines"
                        label={nlsHPCC.Machines}
                        required={true}
                        placeholder={nlsHPCC.SelectValue}
                        onChange={(evt, option) => {
                            setMachine(option.key as string);
                            setOs(option["OS"] as number);
                            onChange(option.key);
                        }}
                        setSetDropzone={_ => setDropzone = _}
                        errorMessage={error && error?.message}
                    />}
                rules={{
                    required: nlsHPCC.ValidationErrorRequired
                }}
            />
            <Controller
                control={control} name="path"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TargetFolderTextField
                        key="path"
                        label={nlsHPCC.Folder}
                        pathSepChar={pathSep}
                        machineAddress={machine}
                        machineDirectory={directory}
                        machineOS={os}
                        required={true}
                        placeholder={nlsHPCC.SelectValue}
                        onChange={(evt, option) => {
                            onChange(option.key);
                        }}
                        errorMessage={error && error?.message}
                    />}
                rules={{
                    required: nlsHPCC.ValidationErrorRequired,
                    pattern: {
                        value: /^(\/[a-z0-9]*)+$/i,
                        message: nlsHPCC.ValidationErrorTargetNameRequired
                    }
                }}
            />
        </Stack>
        <Stack>
            <table className={`${componentStyles.twoColumnTable} ${componentStyles.selectionTable}`}>
                <thead>
                    <tr>
                        <th>#</th>
                        <th>{nlsHPCC.Type}</th>
                        <th>{nlsHPCC.FileName}</th>
                        <th>{nlsHPCC.Size}</th>
                    </tr>
                </thead>
                <tbody>
                    {selection && selection.map((file, idx) => {
                        return <tr key={`File-${idx}`}>
                            <td>{idx + 1}</td>
                            <td>{file["name"].substr(file["name"].lastIndexOf(".") + 1).toUpperCase()}</td>
                            <td>{file["name"]}</td>
                            <td>{`${(parseInt(file["size"], 10) / 1024).toFixed(2)} kb`}</td>
                        </tr>;
                    })}
                </tbody>
            </table>
            <Controller
                control={control} name="overwrite"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Overwrite} />}
            />
        </Stack>
    </MessageBox>;
};