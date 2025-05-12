/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

//nothor
//noroxie

#onwarning (0, ignore); // suppress info re. DeleteLogicalFile

import Std.File;

defaultLz := File.GetLandingZones()[1];
defaultLzName := defaultLz.name;
defaultLzPath := defaultLz.path;

// NB: in containerized the default LZ will have no hostname. i.e. this will be blank, in absence of plane name, RemoteDirectory will find plane via path
defaultLzHost := defaultLz.hostname;

ds := DATASET([{'blah'}], {string f1});


filename := 'file_'+WORKUNIT;
lfnName := '~remotedirtest::' + filename;
dstDesprayedFilename1 := defaultLzPath + '/' + filename;
dstDesprayedFilename2 := defaultLzPath + '/remotedirtest/' + filename;

remoteDirContents1 := File.RemoteDirectory(defaultLzHost, defaultLzPath, '*'+WORKUNIT);
remoteDirContents2 := File.RemoteDirectory('', defaultLzPath, '*'+WORKUNIT, false, defaultLzName);
remoteDirContents3 := File.RemoteDirectory('', defaultLzPath+'/remotedirtest', '*'+WORKUNIT, false, defaultLzName);
remoteDirContents4 := File.RemoteDirectory('', defaultLzPath+'/nonexistent', '*', false, defaultLzName); // should be empty result
remoteDirContents5 := CATCH(File.RemoteDirectory('', defaultLzPath, '*', false, 'unknownPlaneName'), ONFAIL(TRANSFORM(File.FsFilenameRecord,
                                 SELF.name := FAILMESSAGE,
                                 SELF.size := 0,
                                 SELF.modified := ''
                                )));
remoteDirContents6 := CATCH(File.RemoteDirectory('', '/nondropzonepath', '*', false, defaultLzName), ONFAIL(TRANSFORM(File.FsFilenameRecord,
                                 SELF.name := FAILMESSAGE,
                                 SELF.size := 0,
                                 SELF.modified := ''
                                )));
remoteDirContents7 := CATCH(File.RemoteDirectory('10.10.10.1', defaultLzPath, '*'), ONFAIL(TRANSFORM(File.FsFilenameRecord,
                                 SELF.name := FAILMESSAGE,
                                 SELF.size := 0,
                                 SELF.modified := ''
                                )));

SEQUENTIAL(
 OUTPUT(ds, , lfnName);
 File.Despray(logicalName:=lfnName, destinationIP:='', destinationPath:=dstDesprayedFilename1, destinationPlane:=defaultLzName);
 File.Despray(logicalName:=lfnName, destinationIP:='', destinationPath:=dstDesprayedFilename2, destinationPlane:=defaultLzName);

 OUTPUT(remoteDirContents1[1].name=filename);
 OUTPUT(remoteDirContents2[1].name=filename);
 OUTPUT(remoteDirContents3[1].name=filename);
 OUTPUT(remoteDirContents4);

// MORE: CATCH fails to handle these failure cases, so comment out for now
// OUTPUT(remoteDirContents5);
// OUTPUT(remoteDirContents6);
// OUTPUT(remoteDirContents7);

 File.DeleteExternalFile(location:='', path:=dstDesprayedFilename1, planeName:=defaultLzName);
 File.DeleteExternalFile(location:='', path:=dstDesprayedFilename2, planeName:=defaultLzName);
);
