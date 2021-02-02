import { Workunit, WUStateID } from "@hpcc-js/comms";
import Step from "@material-ui/core/Step";
import StepLabel from "@material-ui/core/StepLabel";
import Stepper from "@material-ui/core/Stepper";
import { MuiThemeProvider } from "@material-ui/core/styles";
import nlsHPCC from "../nlsHPCC";
import { theme } from "./theme";
import * as React from "react";

const Steps = [
    {
        text: nlsHPCC.Created,
        activeText: nlsHPCC.Creating
    },
    {
        text: nlsHPCC.Compiled,
        activeText: nlsHPCC.Compiling
    },
    {
        text: nlsHPCC.Executed,
        activeText: nlsHPCC.Executing
    },
    {
        text: nlsHPCC.Completed,
        activeText: nlsHPCC.Completed
    }
];

const wuSteps = (compile: boolean) => {
    return compile ? [Steps[0], Steps[1], Steps[3]] : [...Steps];
};

const wuStep = (wu?: Workunit): number => {
    switch (wu ? wu.StateID : WUStateID.Unknown) {
        case WUStateID.Blocked:
        case WUStateID.Wait:
        case WUStateID.Scheduled:
        case WUStateID.UploadingFiled:
            return 0;
        case WUStateID.Compiling:
            return 1;
        case WUStateID.Submitted:
            return 0;
        case WUStateID.Compiled:
            return wu.ActionEx === "compile" ? 4 : 1;
        case WUStateID.Aborting:
        case WUStateID.Running:
            return 2;
        case WUStateID.Aborted:
            return 4;
        case WUStateID.Archived:
            return 4;
        case WUStateID.Completed:
            return 4;
        case WUStateID.Failed:
            return 4;
        case WUStateID.DebugPaused:
        case WUStateID.DebugRunning:
        case WUStateID.Paused:
        case WUStateID.Unknown:
        default:
            return 0;
    }
};

interface WUStatus {
    wuid: string;
}

export const WUStatus: React.FunctionComponent<WUStatus> = ({
    wuid
}) => {
    const [activeStep, setActiveStep] = React.useState(-1);
    const [failed, setFailed] = React.useState(false);
    const [steps, setSteps] = React.useState([]);

    React.useEffect(() => {
        const wu = Workunit.attach({ baseUrl: "" }, wuid);
        const wuWatchHandle = wu.watch(() => {
            setActiveStep(wuStep(wu));
            setFailed(wu.isFailed());
            setSteps(wuSteps(wu.ActionEx === "compile"));
        });
        wu.refresh(true);
        return () => {
            wuWatchHandle.release();
        };
    }, [wuid]);

    return (
        <MuiThemeProvider theme={theme}>
            <Stepper activeStep={activeStep} alternativeLabel>
                {steps.map((step, i) => {
                    const labelProps = {
                        error: i <= activeStep ? failed : false
                    };
                    const label = activeStep === i ? step.activeText : step.text;
                    return <Step key={i}>
                        <StepLabel {...labelProps}>{label}</StepLabel>
                    </Step>;
                })}
            </Stepper>
        </MuiThemeProvider>
    );
};
