import * as React from "react";
import { DocumentCard, DocumentCardActivity, DocumentCardDetails, DocumentCardPreview, DocumentCardTitle, DocumentCardType, getTheme } from "@fluentui/react";
import { Level } from "@hpcc-js/util";

const theme = getTheme();
const { semanticColors, fonts } = theme;

function iconName(level: Level): string {
    switch (level) {
        case Level.debug:
            return "BugSolid";
        case Level.info:
        case Level.notice:
            return "InfoSolid";
        case Level.warning:
            return "WarningSolid";
        case Level.error:
            return "StatusErrorFull";
        case Level.critical:
        case Level.alert:
        case Level.emergency:
            return "AlertSolid";
    }
}

function iconColor(level: Level): string {
    switch (level) {
        case Level.debug:
            return semanticColors.successIcon;
        case Level.info:
        case Level.notice:
            return semanticColors.infoIcon;
        case Level.warning:
            return semanticColors.warningIcon;
        case Level.error:
            return semanticColors.errorIcon;
        case Level.critical:
        case Level.alert:
        case Level.emergency:
            return semanticColors.severeWarningIcon;
    }
}

export function toasterScale(len: number = 1) {
    return len * 80 / 100;
}

export function invToasterScale(len: number = 1) {
    return len / toasterScale();
}

export interface CustomToasterProps {
    id: string,
    level: Level,
    message: string,
    dateTime: string,
}

export const CustomToaster: React.FunctionComponent<CustomToasterProps> = ({
    id,
    level,
    message,
    dateTime
}) => {
    return <div style={{ transform: `scale(${toasterScale()})` }}>
        <DocumentCard type={DocumentCardType.compact} styles={{ root: { minWidth: 360, height: 90 } }}>
            <DocumentCardPreview previewImages={[{
                previewIconProps: {
                    iconName: iconName(level),
                    styles: {
                        root: {
                            fontSize: fonts.superLarge.fontSize,
                            color: iconColor(level)
                        },
                    },
                },
                width: 72,
            }]} />
            <DocumentCardDetails>
                <DocumentCardTitle title={message} showAsSecondaryTitle />
                <DocumentCardActivity activity={dateTime} people={[{
                    name: id,
                    profileImageSrc: ""
                }]} />
            </DocumentCardDetails>
        </DocumentCard>
    </div>;
};
