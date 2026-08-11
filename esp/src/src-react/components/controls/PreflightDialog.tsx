import * as React from "react";
import { Fields } from "../forms/Fields";
import { FormDialog } from "../forms/FormDialog";
import nlsHPCC from "src/nlsHPCC";
import { pushUrl } from "../../util/history";

export type PreflightMode = "targetCluster" | "clusterProcess" | "systemServer";

export interface PreflightTarget {
    Name: string;
    Type?: string;
    // clusterProcess mode — machine-level details for building the Addresses request
    Netaddress?: string;
    MachineType?: string;
    ParentName?: string;
    ParentDirectory?: string;
}

export interface PreflightFormValues {
    GetProcessorInfo: boolean;
    GetStorageInfo: boolean;
    LocalFileSystemsOnly: boolean;
    GetSoftwareInfo: boolean;
    ApplyProcessFilter: boolean;
    AddProcessesToFilter: string;
    cbAutoRefresh: boolean;
    AutoRefresh: number;
    CpuThreshold: string;
    MemThreshold: string;
    MemThresholdType: string;
    DiskThreshold: string;
    DiskThresholdType: string;
}

const defaultValues: PreflightFormValues = {
    GetProcessorInfo: true,
    GetStorageInfo: true,
    LocalFileSystemsOnly: true,
    GetSoftwareInfo: true,
    ApplyProcessFilter: true,
    AddProcessesToFilter: "",
    cbAutoRefresh: true,
    AutoRefresh: 5,
    CpuThreshold: "95",
    MemThreshold: "95",
    MemThresholdType: "0",
    DiskThreshold: "5",
    DiskThresholdType: "0"
};

interface PreflightDialogProps {
    open: boolean;
    onClose: () => void;
    mode?: PreflightMode;
    selectedTargets: PreflightTarget[];
    url: string;
}

export const PreflightDialog: React.FunctionComponent<PreflightDialogProps> = ({
    open,
    onClose,
    mode = "targetCluster",
    selectedTargets,
    url
}) => {
    const buildFields = React.useCallback((values: PreflightFormValues): Fields => ({
        "GetProcessorInfo": { label: nlsHPCC.ProcessorInformation, type: "checkbox", value: values.GetProcessorInfo },
        "GetStorageInfo": { label: nlsHPCC.StorageInformation, type: "checkbox", value: values.GetStorageInfo },
        "LocalFileSystemsOnly": { label: nlsHPCC.LocalFileSystemsOnly, type: "checkbox", value: values.LocalFileSystemsOnly },
        "GetSoftwareInfo": { label: nlsHPCC.GetSoftwareInformation, type: "checkbox", value: values.GetSoftwareInfo },
        "ApplyProcessFilter": { label: nlsHPCC.ShowProcessesUsingFilter, type: "checkbox", value: values.ApplyProcessFilter },
        "AddProcessesToFilter": { label: nlsHPCC.AddtionalProcessesToFilter, type: "string", value: values.AddProcessesToFilter, placeholder: nlsHPCC.AnyAdditionalProcessesToFilter },
        "cbAutoRefresh": { label: nlsHPCC.AutoRefresh, type: "checkbox", value: values.cbAutoRefresh },
        "AutoRefresh": { label: nlsHPCC.AutoRefreshIncrement, type: "number", value: values.AutoRefresh, placeholder: nlsHPCC.AutoRefreshEvery },
        "CpuThreshold": { label: nlsHPCC.WarnIfCPUUsageIsOver, type: "string", value: values.CpuThreshold, placeholder: nlsHPCC.EnterAPercentage },
        "MemThreshold": { label: nlsHPCC.WarnIfAvailableMemoryIsUnder, type: "string", value: values.MemThreshold, placeholder: nlsHPCC.EnterAPercentageOrMB },
        "MemThresholdType": {
            label: nlsHPCC.Type,
            type: "dropdown",
            value: values.MemThresholdType,
            options: [
                { key: "0", text: "%" },
                { key: "1", text: "MB" }
            ]
        },
        "DiskThreshold": { label: nlsHPCC.WarnIfAvailableDiskSpaceIsUnder, type: "string", value: values.DiskThreshold, placeholder: nlsHPCC.EnterAPercentageOrMB },
        "DiskThresholdType": {
            label: nlsHPCC.Type,
            type: "dropdown",
            value: values.DiskThresholdType,
            options: [
                { key: "0", text: "%" },
                { key: "1", text: "MB" }
            ]
        }
    }), []);

    const submitPreflightRequest = React.useCallback((values: PreflightFormValues) => {
        const params = new URLSearchParams();
        params.set("AutoRefresh", String(values.AutoRefresh));
        params.set("MemThreshold", values.MemThreshold);
        params.set("CpuThreshold", values.CpuThreshold);
        params.set("MemThresholdType", values.MemThresholdType);
        params.set("DiskThreshold", values.DiskThreshold);
        params.set("DiskThresholdType", values.DiskThresholdType);
        if (values.GetProcessorInfo) params.set("GetProcessorInfo", "true");
        if (values.GetStorageInfo) {
            params.set("GetStorageInfo", "true");
            if (values.LocalFileSystemsOnly) params.set("LocalFileSystemsOnly", "true");
        }
        if (values.GetSoftwareInfo) {
            params.set("GetSoftwareInfo", "true");
            if (values.ApplyProcessFilter) {
                params.set("ApplyProcessFilter", "true");
                if (values.AddProcessesToFilter.trim()) params.set("AddProcessesToFilter", values.AddProcessesToFilter.trim());
            }
        }

        if (mode === "targetCluster") {
            selectedTargets.map(t => t.Type ? `${t.Type}:${t.Name}` : t.Name)
                .forEach(tc => params.append("TargetClusters", tc));
        } else if (mode === "systemServer") {
            // Format expected by GetMachineInfo: "netaddress|:MachineType:ParentName:2:ParentDirectory:idx"
            selectedTargets.forEach((target, idx) => {
                const addr = `${target.Netaddress ?? ""}|:${target.MachineType ?? ""}:${target.ParentName ?? ""}:2:${target.ParentDirectory ?? ""}:${idx}`;
                params.append("Addresses", addr);
            });
        } else {
            // Format expected by GetMachineInfo: "netaddress|:MachineType:ParentName:2:ParentDirectory:idx"
            selectedTargets.forEach((target, idx) => {
                const addr = `${target.Netaddress ?? ""}|:${target.MachineType ?? ""}:${target.ParentName ?? ""}:2:${target.ParentDirectory ?? ""}:${idx}`;
                params.append("Addresses", addr);
            });
        }
        pushUrl(`${url}?${params.toString()}`);

        onClose();
    }, [mode, onClose, selectedTargets, url]);

    return <FormDialog<PreflightFormValues>
        open={open}
        title={nlsHPCC.Preflight}
        initialValues={defaultValues}
        buildFields={buildFields}
        onClose={onClose}
        onSubmit={submitPreflightRequest}
    />;
};
