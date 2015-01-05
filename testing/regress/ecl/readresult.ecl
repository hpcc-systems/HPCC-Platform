/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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

//noroxie       - roxie doesn't currently support reading from external workunits

namesRecord := 
            RECORD
string10        forename;
string20        surname;
            END;


//Horrible code - get a list of workunits that match the name of the job that creates the result
//which needs to be inside a nothor.

import Std.System.Workunit as Wu;
myWuid := workunit;
startOfDay := myWuid[1..9] + '-000000';
writers := Wu.WorkunitList(lowWuid := startOfDay,jobname := 'aaawriteresult*');

//Now sort and extract the most recent wuid that matches the condition
lastWriter := sort(nothor(writers), -wuid);
wuid := TRIM(lastWriter[1].wuid) : independent;   // trim should not be needed

ds := dataset(workunit(WUID,'ExportedNames'), namesRecord);

p := ds : persist('readExported', single);

import Std.File;
sequential(
    output(NOFOLD(wuid)[1..1]); /// yuk - output the wuid so it gets evaluated before the check in the persist call
    File.DeleteLogicalFile('~readExported'),
    output(p);
);
