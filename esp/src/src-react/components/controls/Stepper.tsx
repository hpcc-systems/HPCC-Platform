import * as React from "react";
import { makeStyles, mergeClasses, tokens } from "@fluentui/react-components";
import { CheckmarkCircle20Filled, Warning20Filled } from "@fluentui/react-icons";

const useStepStyles = makeStyles({
    wrapper: {
        display: "flex",
        flexDirection: "row",
        position: "relative",
        alignItems: "center",
        padding: "3px 8px",
        marginLeft: "1px",
        border: `1px solid ${tokens.colorNeutralForegroundDisabled}`,
        borderRadius: "5px"
    },
    icon: {
        marginRight: "3px"
    },
    stepNumber: {
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        width: "20px",
        height: "20px",
        marginRight: "3px",
        borderRadius: "50%",
        backgroundColor: tokens.colorNeutralForeground1,
        color: tokens.colorNeutralBackground1,
        fontSize: "0.75rem",
        fontWeight: 600
    },
    failed: { color: `${tokens.colorPaletteRedForeground1} !important` },
    completed: {
        color: tokens.colorBrandBackground
    },
    label: {
        fontSize: "0.875rem",
        fontWeight: 500,
        fontFamily: "'Segoe UI', 'Segoe UI Web (West European)', 'Segoe UI', -apple-system, BlinkMacSystemFont, Roboto, 'Helvetica Neue', sans-serif",
    },
    timing: {
        marginLeft: "6px",
        fontSize: "0.75rem",
    },
    connector: {
        height: "2px",
        width: "14px",
        backgroundColor: tokens.colorNeutralForegroundDisabled,
        position: "relative",
        "::after": {
            content: '""',
            position: "absolute",
            right: "-1px",
            top: "-5px",
            width: 0,
            height: 0,
            borderTop: "6px solid transparent",
            borderBottom: "6px solid transparent",
            borderLeft: `6px solid ${tokens.colorNeutralForegroundDisabled}`,
        }
    }
});

const useStepperStyles = makeStyles({
    wrapper: {
        display: "flex",
        flexDirection: "row",
        flexWrap: "wrap",
        alignItems: "center",
        marginLeft: "auto",
        padding: "0 8px 8px 0",
        "@container (max-width: 900px)": {
            margin: "4px 0 0 10px"
        }
    },
});

export interface StepProps {
    label?: string;
    completed?: boolean;
    failed?: boolean;
    step?: number;
    timing?: string;
    showConnector?: boolean;
}

const Step: React.FunctionComponent<StepProps> = ({
    label = "",
    completed = false,
    failed = false,
    step = 1,
    timing = "",
    showConnector = false
}) => {

    const stepStyles = useStepStyles();

    return <>
        {showConnector ? <div className={stepStyles.connector}></div> : ""}
        <div className={stepStyles.wrapper}>
            {failed ?
                <Warning20Filled className={mergeClasses(stepStyles.icon, stepStyles.failed)} /> :
                completed ?
                    <CheckmarkCircle20Filled className={mergeClasses(stepStyles.icon, stepStyles.completed)} /> :
                    <span className={stepStyles.stepNumber}>{step}</span>
            }
            {failed ?
                <span className={mergeClasses(stepStyles.label, stepStyles.failed)}>{label}</span> :
                <>
                    <span className={stepStyles.label}>{label}</span>
                    {timing ? <span className={stepStyles.timing}>{timing}</span> : <></>}
                </>
            }
        </div>
    </>;

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
            const { label, completed, failed, step, timing, showConnector } = { ...props };
            return <Step key={`${label}_${i}`} label={label} step={step} failed={failed} completed={completed} timing={timing} showConnector={showConnector}></Step>;
        })}
    </div >;

};