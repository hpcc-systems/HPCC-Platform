/*
This file contains all the constants that configure how many records will be generated.  It is archived if the results are saved.
*/
export system := MODULE
    export memoryPerSlave := 0x10000000;
    export numSlaves := 3;
    export SplitWidth := 16;  // Number of ways the splitter/appending tests divide
    export simpleRecordCount := (memoryPerSlave * numSlaves) / 32;  // Enough records to ensure memory is filled when writing.
end;
