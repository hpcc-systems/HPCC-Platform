import * as React from "react";
import { Dropdown, TextField, PrimaryButton, Stack, Checkbox } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { scopedLogger } from "@hpcc-js/util";
import { SashaService, WsSasha } from "@hpcc-js/comms";

const logger = scopedLogger("src-react/components/Sasha.tsx");

interface SashaProps { }

export const Sasha: React.FunctionComponent<SashaProps> = () => {
  const [selectedOption, setSelectedOption] = React.useState("");
  const [wuid, setWuid] = React.useState("");
  const [cluster, setCluster] = React.useState("");
  const [owner, setOwner] = React.useState("");
  const [jobName, setJobName] = React.useState("");
  const [stateFilter, setStateFilter] = React.useState("");
  const [fromDate, setFromDate] = React.useState("");
  const [toDate, setToDate] = React.useState("");
  const [beforeWU, setBeforeWU] = React.useState("");
  const [afterWU, setAfterWU] = React.useState("");
  const [outputFields, setOutputFields] = React.useState("");
  const [archived, setArchived] = React.useState(false);
  const [online, setOnline] = React.useState(false);
  const [includeDT, setIncludeDT] = React.useState(false);
  const [descending, setDescending] = React.useState(false);

  const [result, setResult] = React.useState("");

  const sashaService = new SashaService({ baseUrl: "" });

  const handleOptionChange = (event: React.FormEvent<HTMLDivElement>, option: any) => {
    setSelectedOption(option.key);
  };

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    switch (selectedOption) {
      case "getVersion":
        sashaService.GetVersion({ GetVersionRequest: {} })
          .then(response => {
            console.log("GetVersion response", response.Result);
            setResult(response.Result);
          })
          .catch(err => {
            console.error(err);
            setResult(nlsHPCC.Error);
          });
        break;
      case "getLastServerMessage":
        // Implement getLastServerMessage function call
        break;
      case "restoreECLWorkUnit":
        sashaService.RestoreWU({
          Wuid: wuid,
          WUType: WsSasha.WUTypes.ECL
        })
          .then(response => {
            setResult(response.Result);
          })
          .catch(err => logger.error(err));
        break;
      case "restoreDFUWorkUnit":
        sashaService.RestoreWU({
          Wuid: wuid,
          WUType: WsSasha.WUTypes.DFU
        })
          .then(response => {
            setResult(response.Result);
          })
          .catch(err => logger.error(err));
          break;
      case "archiveECLWorkUnit":
        sashaService.ArchiveWU({
          Wuid: wuid,
          WUType: WsSasha.WUTypes.ECL
        })
          .then(response => {
            setResult(response.Result);
          })
          .catch(err => logger.error(err));
        break;
      case "archiveDFUWorkUnit":
        sashaService.ArchiveWU({
          Wuid: wuid,
          WUType: WsSasha.WUTypes.DFU
        })
          .then(response => {
            setResult(response.Result);
          })
          .catch(err => logger.error(err));
        break;
      case "listECLWorkunit":
        sashaService.ListWU({
          WUType: WsSasha.WUTypes.ECL,
          Wuid: wuid,
          Cluster: cluster,
          Owner: owner,
          JobName: jobName,
          State: stateFilter,
          FromDate: fromDate,
          ToDate: toDate,
          Archived: archived,
          Online: online,
          IncludeDT: includeDT,
          BeforeWU: beforeWU,
          AfterWU: afterWU,
          MaxNumberWUs: 500,
          Descending: descending,
          OutputFields: outputFields
        })
          .then(response => {
            setResult(response.Result);
          })
          .catch(err => logger.error(err));
        break;
      case "listDFUWorkunit":
        sashaService.ListWU({
          WUType: WsSasha.WUTypes.DFU,
          Wuid: wuid,
          Cluster: cluster,
          Owner: owner,
          JobName: jobName,
          State: stateFilter,
          FromDate: fromDate,
          ToDate: toDate,
          Archived: archived,
          Online: online,
          IncludeDT: includeDT,
          BeforeWU: beforeWU,
          AfterWU: afterWU,
          MaxNumberWUs: 500,
          Descending: descending,
          OutputFields: outputFields
        })
          .then(response => {
            setResult(response.Result);
          })
          .catch(err => logger.error(err));
        break;
      default:
        console.log("Invalid option selected");
    }
  };

  const defaultValue = result ? null : <div>{nlsHPCC.noDataMessage}</div>;

  return (
    <div>
      <form onSubmit={handleSubmit}>
        <Dropdown
          placeholder={nlsHPCC.SelectAnOption}
          selectedKey={selectedOption}
          onChange={handleOptionChange}
          options={[
            { key: "", text: nlsHPCC.SelectAnOption },
            { key: "getVersion", text: nlsHPCC.GetVersion },
            { key: "getLastServerMessage", text: nlsHPCC.GetLastServerMessage },
            { key: "restoreECLWorkUnit", text: nlsHPCC.RestoreECLWorkunit },
            { key: "restoreDFUWorkUnit", text: nlsHPCC.RestoreDFUWorkunit },
            { key: "archiveECLWorkUnit", text: nlsHPCC.ArchiveECLWorkunit },
            { key: "archiveDFUWorkUnit", text: nlsHPCC.ArchiveDFUWorkunit },
            { key: "listECLWorkunit", text: nlsHPCC.ListECLWorkunit },
            { key: "listDFUWorkunit", text: nlsHPCC.ListDFUWorkunit }
          ]}
          styles={{ dropdown: { width: 400 } }}
        />

        {["listECLWorkunit", "listDFUWorkunit"].includes(selectedOption) ? (
          <Stack tokens={{ childrenGap: 10 }}>
            <TextField
              label={nlsHPCC.WUID}
              value={wuid}
              onChange={(event, newValue?: string) => setWuid(newValue || "")}
              styles={{ fieldGroup: { width: 400 } }}
            />
            <TextField
              label="Cluster"
              value={cluster}
              onChange={(event, newValue?: string) => setCluster(newValue || "")}
              styles={{ fieldGroup: { width: 400 } }}
            />
            <TextField
              label="Owner"
              value={owner}
              onChange={(event, newValue?: string) => setOwner(newValue || "")}
              styles={{ fieldGroup: { width: 400 } }}
            />
            <TextField
              label="Job Name"
              value={jobName}
              onChange={(event, newValue?: string) => setJobName(newValue || "")}
              styles={{ fieldGroup: { width: 400 } }}
            />
            <TextField
              label="State"
              value={stateFilter}
              onChange={(event, newValue?: string) => setStateFilter(newValue || "")}
              styles={{ fieldGroup: { width: 400 } }}
            />
            <TextField
              label="From Date"
              value={fromDate}
              onChange={(event, newValue?: string) => setFromDate(newValue || "")}
              styles={{ fieldGroup: { width: 400 } }}
            />
            <TextField
              label="To Date"
              value={toDate}
              onChange={(event, newValue?: string) => setToDate(newValue || "")}
              styles={{ fieldGroup: { width: 400 } }}
            />
            <TextField
              label="Before WU"
              value={beforeWU}
              onChange={(event, newValue?: string) => setBeforeWU(newValue || "")}
              styles={{ fieldGroup: { width: 400 } }}
            />
            <TextField
              label="After WU"
              value={afterWU}
              onChange={(event, newValue?: string) => setAfterWU(newValue || "")}
              styles={{ fieldGroup: { width: 400 } }}
            />
            <TextField
              label="Output Fields"
              value={outputFields}
              onChange={(event, newValue?: string) => setOutputFields(newValue || "")}
              styles={{ fieldGroup: { width: 400 } }}
            />
            <Stack horizontal tokens={{ childrenGap: 20 }}>
              <Checkbox
                label="Archived"
                checked={archived}
                onChange={(e, checked) => setArchived(!!checked)}
              />
              <Checkbox
                label="Online"
                checked={online}
                onChange={(e, checked) => setOnline(!!checked)}
              />
              <Checkbox
                label="Include DT"
                checked={includeDT}
                onChange={(e, checked) => setIncludeDT(!!checked)}
              />
              <Checkbox
                label="Descending"
                checked={descending}
                onChange={(e, checked) => setDescending(!!checked)}
              />
            </Stack>
          </Stack>
        ) : (
          (["restoreECLWorkUnit", "restoreDFUWorkUnit", "archiveECLWorkUnit", "archiveDFUWorkUnit"].includes(selectedOption)) && (
            <TextField
              label={nlsHPCC.WUID}
              value={wuid}
              onChange={(event, newValue?: string) => setWuid(newValue || "")}
              styles={{ fieldGroup: { width: 400 } }}
            />
          )
        )}

        <PrimaryButton type="submit" style={{ marginTop: 10, width: 150 }}>
          {nlsHPCC.Submit}
        </PrimaryButton>
        {defaultValue}
        {result && <div>{nlsHPCC.Results}: {result}</div>}
      </form>
    </div>
  );
};