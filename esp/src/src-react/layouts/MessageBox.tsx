import * as React from "react";
import { mergeStyleSets, IDragOptions, IIconProps, ContextualMenu, IconButton, Modal, useTheme, IStackTokens, IModalStyles } from "@fluentui/react";
import { StackShim } from "@fluentui/react-migration-v8-v9";
import nlsHPCC from "src/nlsHPCC";

const headerTokens: IStackTokens = {
    childrenGap: 8,
    padding: "0px 12px 0px 12px"
};

const footerTokens: IStackTokens = {
    childrenGap: 8,
    padding: "12px 12px 12px 12px"
};

const dragOptions: IDragOptions = {
    dragHandleSelector: ".draggable",
    moveMenuItemText: nlsHPCC.Move,
    closeMenuItemText: nlsHPCC.Close,
    menu: ContextualMenu,
};

const cancelIcon: IIconProps = { iconName: "Cancel" };

const iconButtonStyles = {
    root: {
        marginLeft: "auto",
        marginTop: "4px",
        marginRight: "2px",
    },
};

//  Workaround for:  https://github.com/microsoft/fluentui/issues/22878, https://github.com/microsoft/fluentui/issues/23363, https://github.com/microsoft/fluentui/issues/22878
const modalStyles: Partial<IModalStyles> = {
    root: {
        position: "fixed",
        top: 0,
        left: 0,
        pointerEvents: "none"
    },
    main: {
        pointerEvents: "auto"
    }
};

interface MessageBoxProps {
    title: string;
    minWidth?: number;
    show: boolean;
    modeless?: boolean;
    blocking?: boolean;
    onDismiss?: () => void;
    setShow: (_: boolean) => void;
    footer?: React.ReactNode;
    children?: React.ReactNode;
    disableClose?: boolean;
}

export const MessageBox: React.FunctionComponent<MessageBoxProps> = ({
    title,
    minWidth = 360,
    show,
    modeless = true,
    blocking = true,
    onDismiss,
    setShow,
    footer,
    children,
    disableClose = false
}) => {

    const theme = useTheme();
    const contentStyles = React.useMemo(() => mergeStyleSets({
        container: { display: "flex", overflowY: "hidden", minWidth: minWidth },
        header: { borderTop: `4px solid ${theme.palette.themePrimary}`, cursor: "move" },
        body: { padding: "12px 24px 12px 24px", overflowY: "hidden" },
    }), [theme.palette.themePrimary, minWidth]);

    const close = React.useCallback(() => {
        if (disableClose) return;
        if (onDismiss) {
            onDismiss();
        }
        setShow(false);
    }, [disableClose, onDismiss, setShow]);

    return <Modal isOpen={show} onDismiss={close} isModeless={modeless} dragOptions={dragOptions}
        isBlocking={blocking} containerClassName={contentStyles.container} styles={modalStyles}>
        <StackShim tokens={headerTokens} horizontal horizontalAlign="space-between" verticalAlign="center" styles={{ root: contentStyles.header }} className="draggable">
            <h2>{title}</h2>
            <IconButton iconProps={cancelIcon} ariaLabel={nlsHPCC.CloseModal} onClick={close} styles={iconButtonStyles} disabled={disableClose} />
        </StackShim>
        <div className={contentStyles.body}>
            {children}
        </div>
        <StackShim tokens={footerTokens} horizontal horizontalAlign="end">
            {footer}
        </StackShim>
    </Modal>;
};
