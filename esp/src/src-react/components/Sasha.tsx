import * as React from "react";
import { Dropdown, TextField, PrimaryButton } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { scopedLogger } from "@hpcc-js/util";
import { SashaService, WsSasha } from "@hpcc-js/comms";

const logger = scopedLogger("src-react/components/Sasha.tsx");

interface SashaProps { }

export const Sasha: React.FunctionComponent<SashaProps> = () => {
  const [selectedOption, setSelectedOption] = React.useState("");
  const [wuid, setWuid] = React.useState("");
  const [result, setResult] = React.useState("");

  // Create an instance of SashaService (adjust baseUrl if needed)
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
        // Implement restoreECLWorkUnit function call
        break;
      case "restoreDFUWorkUnit":
        // Implement restoreDFUWorkUnit function call
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
      case "backupECLWorkUnit":
        // Implement backupECLWorkUnit function call
        break;
      case "backupDFUWorkUnit":
        // Implement backupDFUWorkUnit function call
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
            { key: "backupECLWorkUnit", text: nlsHPCC.BackupECLWorkunit },
            { key: "backupDFUWorkUnit", text: nlsHPCC.BackupDFUWorkunit }
          ]}
          styles={{ dropdown: { width: 400 } }}
        />
        {["restoreECLWorkUnit", "restoreDFUWorkUnit", "archiveECLWorkUnit", "archiveDFUWorkUnit", "backupECLWorkUnit", "backupDFUWorkUnit"].includes(selectedOption) && (
          <div>
            <TextField
              label={nlsHPCC.WUID}
              value={wuid}
              onChange={(event: React.FormEvent<HTMLInputElement>, newValue?: string) => setWuid(newValue || "")}
              styles={{ fieldGroup: { width: 400 } }}
            />
          </div>
        )}
        <PrimaryButton type="submit">{nlsHPCC.Submit}</PrimaryButton>
        {defaultValue}
        {result && <div>{nlsHPCC.Results}: {result}</div>}
      </form>
    </div>
  );
};