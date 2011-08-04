/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
FileServices.DeleteSuperFile('TESTSUPER::SubSuper'+%I%,);
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
