import * as React from "react";
import { DefaultButton, Dialog, DialogFooter, PrimaryButton } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";

interface ConfirmProps {
    show: boolean;
    setShow: (_: boolean) => void;
    title: string;
    message: string;
    onSubmit: () => void;
}

export const Confirm: React.FunctionComponent<ConfirmProps> = ({
    show,
    setShow,
    title,
    message,
    onSubmit = null
}) => {

    return <Dialog
        hidden={!show}
        onDismiss={() => setShow(false)}
        dialogContentProps={{
            title: title
        }}
        maxWidth={500}
    >
        <div>
            {message.split("\n").map(str => {
                return <span>{str} <br /></span>;
            })}
        </div>
        <DialogFooter>
            <PrimaryButton text={nlsHPCC.OK}
                onClick={() => {
                    if (typeof onSubmit === "function") {
                        onSubmit();
                    }
                    setShow(false);
                }}
            />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => setShow(false)} />
        </DialogFooter>
    </Dialog>;

};