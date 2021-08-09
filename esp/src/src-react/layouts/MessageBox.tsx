import * as React from "react";
import { mergeStyleSets, IDragOptions, IIconProps, ContextualMenu, IconButton, Modal, Stack, useTheme, IStackTokens } from "@fluentui/react";
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

interface MessageBoxProps {
    title: string;
    show: boolean;
    setShow: (_: boolean) => void;
    footer?: React.ReactNode;
    children?: React.ReactNode;
}

export const MessageBox: React.FunctionComponent<MessageBoxProps> = ({
    title,
    show,
    setShow,
    footer,
    children
}) => {

    const theme = useTheme();
    const contentStyles = React.useMemo(() => mergeStyleSets({
        container: { overflowY: "hidden" },
        header: { borderTop: `4px solid ${theme.palette.themePrimary}` },
        body: { padding: "12px 24px 12px 24px", overflowY: "hidden" },
    }), [theme.palette.themePrimary]);

    const close = React.useCallback(() => setShow(false), [setShow]);

    return <Modal isOpen={show} onDismiss={close} dragOptions={dragOptions}
        isBlocking={false} containerClassName={contentStyles.container}>
        <Stack tokens={headerTokens} horizontal horizontalAlign="space-between" verticalAlign="center" styles={{ root: contentStyles.header }}>
            <h2>{title}</h2>
            <IconButton iconProps={cancelIcon} ariaLabel={nlsHPCC.CloseModal} onClick={close} styles={iconButtonStyles} />
        </Stack>
        <div className={contentStyles.body}>
            {children}
        </div>
        <Stack tokens={footerTokens} horizontal horizontalAlign="end">
            {footer}
        </Stack>
    </Modal>;
};