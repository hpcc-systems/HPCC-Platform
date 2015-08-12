/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

import lib_fileservices;
Rec := {STRING1 Letter, INTEGER1 Number};
InitFile1 := DATASET([{'A',1},{'B',1},{'C',1},{'D',1},{'E',1},
                     {'F',1},{'G',1},{'H',1},{'I',1},{'J',1},
                     {'K',1},{'L',1},{'M',1},{'N',1},{'O',1},
                     {'P',1},{'Q',1},{'R',1},{'S',1},{'T',1},
                     {'U',1},{'V',1},{'W',1},{'X',1},{'Y',1}],
Rec);


LOADXML('<defaultscope/>');
#declare(ITERATIONS)
#set(ITERATIONS,100)


dotest(unsigned sfnum,unsigned subnum) := FUNCTION
  return sequential(
    FileServices.StartSuperFileTransaction(),
    IF (FileServices.FindSuperFileSubName('TESTSUPER::SubSuper'+sfnum,'TESTSUPER::Sub'+subnum) = 0,
    FileServices.AddSuperFile('TESTSUPER::SubSuper'+sfnum, 'TESTSUPER::Sub'+subnum),
    FileServices.RemoveSuperFile('TESTSUPER::SubSuper'+sfnum, 'TESTSUPER::Sub'+subnum)
    ),

    FileServices.FinishSuperFileTransaction()
  );
END;

#declare(I)

FileServices.DeleteSuperFile('TESTSUPER::Super');

#set(I,1)
#loop
  FileServices.DeleteSuperFile('TESTSUPER::SubSuper'+%I%);
  #set(I,%I%+1)
  #if (%I%>5)
    #break
  #end
#end

#set(I,1)
#loop
  output(InitFile1,,'TESTSUPER::Sub'+%I%,overwrite);
  #set(I,%I%+1)
  #if (%I%>50)
    #break
  #end
#end
FileServices.CreateSuperFile('TESTSUPER::Super');

#set(I,1)
#loop
  FileServices.CreateSuperFile('TESTSUPER::SubSuper'+%I%);
  FileServices.AddSuperFile('TESTSUPER::Super','TESTSUPER::SubSuper'+%I%);
  #set(I,%I%+1)
  #if (%I%>5)
    #break
  #end
#end

#set(I,1)
#loop
  dotest(RANDOM()%5+1,RANDOM()%10+1);
  #set(I,%I%+1)
  #if (%I%>20)
    #break
  #end
#end
