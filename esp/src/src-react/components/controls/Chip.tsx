import * as React from "react";
import { styled } from '@fluentui/utilities';
import { lighten, fade } from "@material-ui/core/styles";

import { IStyleFunctionOrObject, classNamesFunction } from '@fluentui/react';
import { IStyle, ITheme } from '@fluentui/style-utilities';

import { StatusErrorFullIcon } from "@fluentui/react-icons-mdl2";

interface IChipProps extends React.RefAttributes<HTMLDivElement> {
    label: string;
    color?: string;
    onDelete?: Function;
    className?: string;
    theme?: ITheme;
    styles?: IStyleFunctionOrObject<IChipStyleProps, IChipStyles>;
}

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

function getStyles(props: IChipStyleProps): IChipStyles {

    const { theme, color } = props;

    const classNames = {
        root: 'chip-container',
        label: 'chip-label',
        closeIcon: 'chip-close-icon'
    };

    let backgroundColor = theme.palette.themePrimary,
        textColor = theme.palette.white;

    switch (color) {
        case "default":
        case "neutral":
            backgroundColor = theme.palette.neutralLight;
            textColor = theme.palette.neutralDark;
            break;
        case "secondary":
            backgroundColor = theme.palette.themeSecondary;
            break;
    }

    return {
        root: [
            classNames.root,
            {
                color: textColor,
                backgroundColor: backgroundColor,
                height: "2.5rem",
                minWidth: "5rem",
                justifyContent: "center",
                borderRadius: "1.25rem",
                display: "inline-flex",
                alignItems: "center",
                "&:hover": {
                    backgroundColor: lighten(backgroundColor, 0.1),
                    cursor: "pointer"
                },
                "&:active": {
                    transition: "background-color 0.1s ease-in-out",
                    backgroundColor: lighten(backgroundColor, 0.2)
                }
            }
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
                color: fade(textColor, 0.66),
                alignSelf: "baseline",
                margin: "0.1rem 0.66rem 0 -0.2rem",
                "&:hover": {
                    color: textColor
                }
            }
        ]
    }
}

const getClassNames = classNamesFunction<IChipStyleProps, IChipStyles>();

const ChipBase: React.FunctionComponent<IChipProps> = React.forwardRef<
    HTMLDivElement,
    IChipProps
>((props, forwardedRef) => {
    
    const { label, onDelete, theme, color, className, styles } = props;

    const classNames = getClassNames(styles!, {
        theme: theme!,
        className,
        color
    })

    function handleCloseIconClick(evt) {
        evt.preventDefault();
        if (onDelete) {
            onDelete(evt);
        }
    }

    return <>
        <div className={classNames.root}>
            <span className={classNames.chipLabel}>{label}</span>
            { onDelete !== undefined && 
            <a href="" onClick={handleCloseIconClick} className={classNames.chipCloseIcon}>
                <StatusErrorFullIcon />
            </a> }
        </div>
    </>;
});

export const Chip: React.FunctionComponent<IChipProps> = styled<
    IChipProps,
    IChipStyleProps,
    IChipStyles
>(ChipBase, getStyles, undefined, { scope: 'Chip' });