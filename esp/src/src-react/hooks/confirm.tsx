import * as React from "react";
import { DefaultButton, Dialog, DialogFooter, PrimaryButton } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";

interface useConfirmProps {
    title: string;
    message: string;
    onSubmit: () => void;
}

export function useConfirm({ title, message, onSubmit }: useConfirmProps): [React.FunctionComponent, (_: boolean) => void] {

    const [show, setShow] = React.useState(false);

    const Confirm = React.useMemo(() => () => {
        return <Dialog
            hidden={!show}
            onDismiss={() => setShow(false)}
            dialogContentProps={{
                title: title
            }}
            maxWidth={500}
        >
            <div>
                {message.split("\n").map((str, idx) => {
                    return <span key={idx}>{str} <br /></span>;
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
    }, [show, setShow, title, message, onSubmit]);

    return [Confirm, setShow];

}