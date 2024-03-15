import * as React from "react";
import { Dropdown, TextField, PrimaryButton } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";

interface SashaProps {}

export const Sasha: React.FunctionComponent<SashaProps> = ({ }) => {
  const [selectedOption, setSelectedOption] = React.useState("");
  const [wuid, setWuid] = React.useState("");
  const [result, setResult] = React.useState("");

  const handleOptionChange = (event: React.FormEvent<HTMLDivElement>, option: any) => {
    setSelectedOption(option.key);
  };

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    // Perform action based on selected option
    switch (selectedOption) {
      case "getVersion": 
        // Implement getVersion function call
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
        // Implement archiveECLWorkUnit function call
        break;
      case "archiveDFUWorkUnit":
        // Implement archiveDFUWorkUnit function call
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
    // Reset form
    setSelectedOption("");
    setWuid("");
    setResult("");
  };

  // Conditional rendering for default value
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
              label= {nlsHPCC.WUID}
              value={wuid}
              onChange={(event: React.FormEvent<HTMLInputElement>, newValue?: string) => setWuid(newValue || "")}
              styles={{ fieldGroup: { width: 400 } }}
            />
          </div>
        )}
        <PrimaryButton type="submit" >{nlsHPCC.Submit}</PrimaryButton>
        {/* Render defaultValue when result is empty */}
        {defaultValue}
        {/* Render result when available */}
        {result && <div>{nlsHPCC.Results}: {result}</div>}
      </form>
    </div>
  );
};

export default Sasha;
