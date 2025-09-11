// src/components/forms/IssueDialog.tsx
import * as React from "react";
import { Dialog, DialogType, DialogFooter, PrimaryButton, DefaultButton, Dropdown, IDropdownOption, TextField, Checkbox, Stack } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";

export interface IssueDialogProps {
  wuid: string;
  showForm: boolean;
  setShowForm: (v: boolean) => void;
  jiraBaseUrl?: string;
  workunitContext?: {
    owner?: string;
    cluster?: string;
    state?: string;
    jobname?: string;
    description?: string;
  },
  onIssueLinkSaved?: (link: { type: "jira"; url: string; created: string }) => void;
}

type IssueType = "Bug" | "Task" | "Incident" | "Story" | "Other";
type ProblemNature = "Failed Job" | "Slow Performance" | "Unexpected Results" | "Other";

const toLabelOptions = (items: string[]): IDropdownOption[] =>
  items.map(v => ({ key: v, text: v }));

export const IssueDialog: React.FC<IssueDialogProps> = ({
  wuid, showForm, setShowForm, jiraBaseUrl = "",
  workunitContext, onIssueLinkSaved
}) => {
  // Form state
  const [project, setProject] = React.useState<string>("");
  const [workType, setWorkType] = React.useState<IssueType>("Bug");
  const [status, setStatus] = React.useState<string>("To Do");
  const [summary, setSummary] = React.useState<string>("");
  const [components, setComponents] = React.useState<string>("");
  const [description, setDescription] = React.useState<string>("");
  const [problemNature, setProblemNature] = React.useState<ProblemNature>("Failed Job");
  const [includeWUContext, setIncludeWUContext] = React.useState<boolean>(true);
  const [lastCreatedUrl, setLastCreatedUrl] = React.useState<string>("");

  // Seed defaults from the WU context when opened
  React.useEffect(() => {
    if (showForm) {
      const defaultSummary = workunitContext?.jobname
        ? `[${wuid}] ${workunitContext.jobname}`
        : `[${wuid}] ${nlsHPCC.IssueReport}`;

      const wuBits = [
        `WUID: ${wuid}`,
        workunitContext?.owner ? `Owner: ${workunitContext.owner}` : "",
        workunitContext?.cluster ? `Cluster: ${workunitContext.cluster}` : "",
        workunitContext?.state ? `State: ${workunitContext.state}` : "",
      ].filter(Boolean).join("\n");

      setSummary(defaultSummary);
      setDescription(
        (workunitContext?.description ? (workunitContext.description + "\n\n") : "") +
        nlsHPCC.DescribeProblem +
        (wuBits ? `---\n${wuBits}\n` : "")
      );
      setComponents(workunitContext?.cluster ?? "");
      setProblemNature("Failed Job");
      setStatus("To Do");
      setWorkType("Bug");
      setProject("");
      setLastCreatedUrl("");
    }
  }, [showForm, wuid, workunitContext]);

  const appendWUContext = (body: string) => {
    if (!includeWUContext) return body;
    const ctx: string[] = [];
    ctx.push("", "---", nlsHPCC.WorkunitContext);
    ctx.push(`WUID: ${wuid}`);
    if (workunitContext?.owner) ctx.push(`Owner: ${workunitContext.owner}`);
    if (workunitContext?.cluster) ctx.push(`Cluster: ${workunitContext.cluster}`);
    if (workunitContext?.state) ctx.push(`State: ${workunitContext.state}`);
    return body + "\n" + ctx.join("\n");
  };

  const buildJiraUrl = () => {
    const base = jiraBaseUrl.replace(/\/+$/, "");
    const params = new URLSearchParams();
    if (project) params.set("project", project);
    if (workType) params.set("issuetype", workType);
    if (summary) params.set("summary", summary);
    if (description) params.set("description", appendWUContext(description));
    if (components) params.set("components", components);
    params.set("labels", problemNature.replace(/\s+/g, "_"));
    return `${base}/secure/CreateIssueDetails!init.jspa?${params.toString()}`;
  };

  const saveIssueLink = async (url: string) => {
    const record = { type: "jira" as const, url, created: new Date().toISOString() };
    onIssueLinkSaved?.(record);
  };

  const openAndRemember = () => {
    const url = buildJiraUrl();
    window.open(url, "_blank", "noopener,noreferrer");
    setLastCreatedUrl(url);
  };

  return (
    <Dialog
      hidden={!showForm}
      onDismiss={() => setShowForm(false)}
      dialogContentProps={{
        type: DialogType.largeHeader,
        title: nlsHPCC.CreateIssueFromWU,
        subText: nlsHPCC.ChooseProblemNature
      }}
      minWidth={720}
      modalProps={{ isBlocking: false }}
    >
      <Stack tokens={{ childrenGap: 12 }}>
        <Dropdown
          label={nlsHPCC.ProblemNature}
          selectedKey={problemNature}
          onChange={(_, opt) => setProblemNature(opt?.key as ProblemNature)}
          options={toLabelOptions([
            nlsHPCC.FailedJob,
            nlsHPCC.SlowPerformance,
            nlsHPCC.UnexpectedResults,
            nlsHPCC.Other
          ])}
          required
        />
        <TextField
          label={nlsHPCC.Project}
          value={project}
          onChange={(_, v) => setProject(v ?? "")}
          placeholder={nlsHPCC.ProjectPlaceholder}
        />
        <Dropdown
          label={nlsHPCC.WorkType}
          selectedKey={workType}
          onChange={(_, opt) => setWorkType(opt?.key as IssueType)}
          options={toLabelOptions([
            nlsHPCC.Bug,
            nlsHPCC.Task,
            nlsHPCC.Incident,
            nlsHPCC.Story,
            nlsHPCC.Other
          ])}
          required
        />
        <TextField
          label={nlsHPCC.Status}
          value={status}
          onChange={(_, v) => setStatus(v ?? "")}
          placeholder={nlsHPCC.StatusPlaceholder}
          required
        />
        <TextField
          label={nlsHPCC.Summary}
          value={summary}
          onChange={(_, v) => setSummary(v ?? "")}
          required
        />
        <TextField
          label={nlsHPCC.Components}
          value={components}
          onChange={(_, v) => setComponents(v ?? "")}
          placeholder={nlsHPCC.ComponentsPlaceholder}
        />
        <TextField
          label={nlsHPCC.Description}
          value={description}
          onChange={(_, v) => setDescription(v ?? "")}
          multiline
          rows={6}
          required
        />
        <Checkbox
          label={nlsHPCC.IncludeWUContext}
          checked={includeWUContext}
          onChange={(_, c) => setIncludeWUContext(!!c)}
        />
        <Stack horizontal tokens={{ childrenGap: 8 }}>
          <PrimaryButton text={nlsHPCC.CreateInJIRA} onClick={openAndRemember} />
          <DefaultButton text={nlsHPCC.Close} onClick={() => setShowForm(false)} />
        </Stack>
        {lastCreatedUrl && (
          <Stack tokens={{ childrenGap: 8 }}>
            <TextField
              label={nlsHPCC.PasteIssueURL}
              placeholder="https://..."
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  const input = (e.target as HTMLInputElement).value.trim();
                  if (input.startsWith("http")) {
                    saveIssueLink(input);
                    (e.target as HTMLInputElement).value = "";
                  }
                }
              }}
            />
          </Stack>
        )}
      </Stack>
      <DialogFooter>
        <DefaultButton onClick={() => setShowForm(false)} text={nlsHPCC.Done} />
      </DialogFooter>
    </Dialog>
  );
};
