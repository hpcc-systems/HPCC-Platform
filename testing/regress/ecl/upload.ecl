//class=parquet
//nothor,noroxie
//nokey

import Std.File AS FileServices;

string OriginalTextFilesIp := '.' : STORED('OriginalTextFilesIp');
string OriginalTextFilesPath := '' : STORED('OriginalTextFilesEclPath');
DirectoryPath := '~file::' + OriginalTextFilesIp + '::' + OriginalTextFilesPath + '::';  // path of the documents that are used to get sample file
fileName := 'sample1.parquet';

dataRec := RECORD
    DATA1 d;
END;

inFile := dataset(DirectoryPath+fileName, dataRec, THOR);

// Create a logical file
output(inFile, , 'sample1.parquet', OVERWRITE);

// despray the logical file into the dropzone
dropzonePathTemp := '/var/lib/HPCCSystems/mydropzone/' : STORED('dropzonePath');
dropzonePath := dropzonePathTemp + IF(dropzonePathTemp[LENGTH(dropzonePathTemp)]='/', '', '/');

result := FileServices.fDespray(fileName
                                ,'.'   // local IP
                                ,destinationPath := dropzonePath + fileName
                                ,ALLOWOVERWRITE := True
                               );
// Display DFU WUID
output(result);

// Clean-up
FileServices.DeleteLogicalFile(fileName);
