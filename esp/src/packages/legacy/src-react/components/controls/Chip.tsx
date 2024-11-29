import * as React from "react";
import { styled } from "@fluentui/utilities";
import { IStyleFunctionOrObject, classNamesFunction } from "@fluentui/react";
import { IStyle, ITheme } from "@fluentui/style-utilities";
import { StatusErrorFullIcon } from "@fluentui/react-icons-mdl2";

interface IChipStyleProps {
    className?: string;
    theme: ITheme;
    color: string,
}

interface IChipStyles {
    root: IStyle;
    chipLabel: IStyle;
    chipCloseIcon: IStyle;
}

interface IChipProps extends React.RefAttributes<HTMLDivElement> {
    label: string;
    color?: string;
    onDelete?: () => void;
    className?: string;
    theme?: ITheme;
    styles?: IStyleFunctionOrObject<IChipStyleProps, IChipStyles>;
}

function getStyles(props: IChipStyleProps): IChipStyles {

    const { theme, color, className } = props;

    const classNames = {
        root: "chip-container",
        label: "chip-label",
        closeIcon: "chip-close-icon"
    };

    let backgroundColor = theme.palette.themeDarker;
    let textColor = theme.palette.white;
    let iconColor = theme.palette.themePrimary;

    switch (color) {
        case "default":
        case "neutral":
            backgroundColor = theme.palette.neutralLight;
            textColor = theme.palette.neutralDark;
            iconColor = theme.palette.neutralPrimary;
            break;
        case "secondary":
            backgroundColor = theme.palette.themeSecondary;
            break;
    }

    const mergedStyles = {
        root: [
            classNames.root,
            {
                color: textColor,
                backgroundColor: backgroundColor,
                height: "30px",
                minWidth: "5rem",
                justifyContent: "center",
                borderRadius: "1.25rem",
                display: "inline-flex",
                alignItems: "center"
            },
            className
        ],
        chipLabel: [
            classNames.label,
            {
                fontSize: "0.8125rem",
                padding: "0 0.85rem",
                userSelect: "none"
            }
        ],
        chipCloseIcon: [
            classNames.closeIcon,
            {
                color: iconColor,
                alignSelf: "baseline",
                margin: "0.05rem 0.66rem 0 -0.2rem"
            }
        ]
    };

    return mergedStyles;
}

const getClassNames = classNamesFunction<IChipStyleProps, IChipStyles>();

const ChipBase: React.FunctionComponent<IChipProps> = React.forwardRef<
    HTMLDivElement,
    IChipProps
>((props, forwardedRef) => {
    
    const { label, onDelete, theme, color, className, styles } = props;

    const ariaLabel = "Click to remove";

    const classNames = getClassNames(styles!, {
        theme: theme!,
        className,
        color
    });

    function handleCloseIconClick(evt) {
        evt.preventDefault();
        if (onDelete) {
            onDelete();
        }
    }

    return <div className={classNames.root}>
        <span className={classNames.chipLabel}>{label}</span>
        { onDelete !== undefined && 
        <a href="" onClick={handleCloseIconClick} title={ariaLabel} aria-label={ariaLabel} className={classNames.chipCloseIcon}>
            <StatusErrorFullIcon />
        </a> }
    </div>;
});

export const Chip: React.FunctionComponent<IChipProps> = styled<
    IChipProps,
    IChipStyleProps,
    IChipStyles
>(ChipBase, getStyles, undefined, { scope: "Chip" });