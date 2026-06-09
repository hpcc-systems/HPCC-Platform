import * as React from "react";
import { makeStyles, tokens } from "@fluentui/react-components";

const useStepStyles = makeStyles({
    wrapper: {
        display: "flex",
        flexDirection: "column",
        position: "relative",
        alignItems: "center",
        padding: "0 8px",
        minWidth: "100px",
    },
    svg: {
        color: tokens.colorNeutralForeground1,
        fill: "currentColor",
        width: "1em",
        height: "1em",
        fontSize: "1.5rem",
        "& text": { color: tokens.colorNeutralBackground1 }
    },
    failed: { color: `${tokens.colorPaletteRedForeground1} !important` },
    completed: {
        color: tokens.colorBrandBackground,
        "& circle": { color: tokens.colorNeutralBackground1 }
    },
    label: {
        margin: "16px 0 0 0",
        fontSize: "0.875rem",
        fontWeight: 500,
        fontFamily: "'Segoe UI', 'Segoe UI Web (West European)', 'Segoe UI', -apple-system, BlinkMacSystemFont, Roboto, 'Helvetica Neue', sans-serif",
    },
    connector: {
        top: "12px",
        left: "calc(-50% + 20px)",
        right: "calc(50% + 20px)",
        position: "absolute",
        borderTopStyle: "solid",
        borderTopWidth: "1px"
    }
});

const useStepperStyles = makeStyles({
    wrapper: {
        display: "flex",
        flexDirection: "row",
        padding: "8px"
    },
});

export interface StepProps {
    label?: string;
    completed?: boolean;
    failed?: boolean;
    step?: number;
    showConnector?: boolean;
}

const Step: React.FunctionComponent<StepProps> = ({
    label = "",
    completed = false,
    failed = false,
    step = 1,
    showConnector = false
}) => {

    const stepStyles = useStepStyles();

    return <div className={stepStyles.wrapper}>
        {showConnector ? <div className={stepStyles.connector}></div> : ""}
        {failed ?
            <svg className={[stepStyles.svg, stepStyles.failed].join(" ")} viewBox={"0 0 24 24"}>
                <path d="M1 21h22L12 2 1 21zm12-3h-2v-2h2v2zm0-4h-2v-4h2v4z"></path>
            </svg> :
            completed ?
                <svg className={[stepStyles.svg, stepStyles.completed].join(" ")} viewBox={"0 0 24 24"}>
                    <circle cx="12" cy="12" r="12"></circle>
                    <path d="M12 0a12 12 0 1 0 0 24 12 12 0 0 0 0-24zm-2 17l-5-5 1.4-1.4 3.6 3.6 7.6-7.6L19 8l-9 9z"></path>
                </svg> :
                <svg className={stepStyles.svg} viewBox={"0 0 24 24"}>
                    <circle cx="12" cy="12" r="12"></circle>
                    <text x="7" y="18">{step}</text>
                </svg>
        }
        {failed ?
            <span className={[stepStyles.failed, stepStyles.label].join(" ")}>{label}</span> :
            <span className={stepStyles.label}>{label}</span>
        }
    </div>;

};

export type Orientation = "horizontal" | "vertical";

interface StepperProps {
    activeStep?: number;
    steps: StepProps[];
    orientation?: Orientation;
}

export const Stepper: React.FunctionComponent<StepperProps> = ({
    activeStep = 0,
    steps,
    orientation = "horizontal",
}) => {
    const stepperStyles = useStepperStyles();

    return <div className={stepperStyles.wrapper}>
        {steps && steps.map((props, i) => {
            const { label, completed, failed, step, showConnector } = { ...props };
            return <Step key={`${label}_${i}`} label={label} step={step} failed={failed} completed={completed} showConnector={showConnector}></Step>;
        })}
    </div >;

};