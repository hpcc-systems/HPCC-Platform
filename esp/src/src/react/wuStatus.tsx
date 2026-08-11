import * as React from "react";
import { Workunit, WUStateID } from "@hpcc-js/comms";
import { StepProps, Stepper } from "../../src-react/components/controls/Stepper";
import nlsHPCC from "../nlsHPCC";

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
    wuid?: string;
    workunit?: Workunit;
}

const stepTimings = (wu: Workunit, isCompile: boolean): string[] => {
    const timers = wu.CTimers ?? [];
    const compileTimer = timers.find(t => t.Name === "compile" || t.Name === ">compile");
    const executeTimer = timers.find(t => t.Name === "Total cluster time" || t.Name === "Total thor time");
    if (isCompile) {
        // Steps: Created(0), Compiled(1), Completed(2)
        return ["", compileTimer?.Value ?? "", ""];
    }
    // Steps: Created(0), Compiled(1), Executed(2), Completed(3)
    return ["", compileTimer?.Value ?? "", executeTimer?.Value ?? "", ""];
};

export const WUStatus: React.FunctionComponent<WUStatus> = ({
    wuid,
    workunit
}) => {
    const [activeStep, setActiveStep] = React.useState(-1);
    const [failed, setFailed] = React.useState(false);
    const [stepProps, setStepProps] = React.useState<StepProps[]>();
    const [steps, setSteps] = React.useState([]);
    const [timing, setTiming] = React.useState<string[]>([]);

    React.useEffect(() => {
        if (!wuid && !workunit) {
            return;
        }
        const updateWUStatus = (wu: Workunit) => {
            setActiveStep(wuStep(wu));
            setFailed(wu.isFailed());
            setSteps(wuSteps(wu.ActionEx === "compile"));
            setTiming(stepTimings(wu, wu.ActionEx === "compile"));
        };
        const wu = workunit ?? Workunit.attach({ baseUrl: "" }, wuid);
        updateWUStatus(wu);

        const wuWatchHandle = wu.watch(() => updateWUStatus(wu));

        if (!workunit) {
            wu.refresh(true, { IncludeTimers: true });
        }

        return () => {
            wuWatchHandle.release();
        };
    }, [workunit, wuid]);

    React.useEffect(() => {
        setStepProps(steps.map((step, i) => {
            const label = activeStep === i ? step.activeText : step.text;
            return {
                key: `${label}_${i}`,
                label: label,
                step: i + 1,
                failed: failed,
                completed: activeStep > i,
                showConnector: i > 0 && i < steps.length + 1,
                timing: timing[i]
            };
        }));
    }, [activeStep, failed, steps, timing]);

    return <Stepper activeStep={activeStep} steps={stepProps}></Stepper>;
};
