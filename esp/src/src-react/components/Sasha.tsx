import * as React from "react";
import { Dropdown, IDropdownOption, TextField, PrimaryButton } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { SashaService, WsSasha } from "@hpcc-js/comms";

interface SashaProps {}

const mySashaService = new SashaService({ baseUrl: "" });

export const Sasha: React.FunctionComponent<SashaProps> = () => {
  const [selectedOption, setSelectedOption] = React.useState<string>("");
  const [wuid, setWuid] = React.useState<string>("");
  const [result, setResult] = React.useState<string>("");

  const handleOptionChange = (event: React.FormEvent<HTMLDivElement>, option?: IDropdownOption) => {
    setSelectedOption(option?.key as string);
  };

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    switch (selectedOption) {
      case "archiveECLWorkUnit":
        await handleArchiveWorkUnit("ECL");
        break;
      case "archiveDFUWorkUnit":
        await handleArchiveWorkUnit("DFU");
        break;
      default:
        console.log("Invalid option selected");
    }
    setSelectedOption("");
    setWuid("");
  };

  const handleArchiveWorkUnit = async (wuType: string) => {
    try {
      const response = await mySashaService.ArchiveWU({ Wuid: wuid, WUType: wuType as WsSasha.WUTypes });
      setResult(response.Result);
    } catch (error) {
      console.error('Error:', error);
      setResult(nlsHPCC.noDataMessage);
    }
  };

  return (
    <HolyGrail
      main={
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
            {(selectedOption === "archiveECLWorkUnit" || selectedOption === "archiveDFUWorkUnit") && (
              <div>
                <TextField
                  label={nlsHPCC.WUID}
                  value={wuid}
                  onChange={(event, newValue) => setWuid(newValue || "")}
                  styles={{ fieldGroup: { width: 400 } }}
                />
              </div>
            )}
            <PrimaryButton type="submit">{nlsHPCC.Submit}</PrimaryButton>
            {/* Render result when available */}
            {result && <div>{nlsHPCC.Results}: {result}</div>}
          </form>
        </div>
      }
    />
  );
};

export default Sasha;