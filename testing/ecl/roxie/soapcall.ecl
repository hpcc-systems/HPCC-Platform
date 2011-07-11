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


d := dataset([{'FRED'},{'WILMA'}], {string unkname});

ServiceOutRecord := 
    RECORD
        string name;
        data pic;
        unsigned4 id{xpath('r')};
        unsigned4 novalue;
    END;

// simple query->dataset form
output(SORT(SOAPCALL('http://127.0.0.1:9876','soapbase', { string unkname := 'FRED' }, dataset(ServiceOutRecord)),record));

// double query->dataset form
output(SORT(SOAPCALL('http://127.0.0.1:9876|http://127.0.0.1:9876','soapbase', { string unkname := 'FRED' }, dataset(ServiceOutRecord)),record));

// simple dataset->dataset form
output(sort(SOAPCALL(d, 'http://127.0.0.1:9876','soapbase', { unkname }, DATASET(ServiceOutRecord)),record));

// double query->dataset form
ServiceOutRecord doError(d l) := TRANSFORM
  SELF.name := 'ERROR: ' + if (l.unkname='FRED' AND failmessage='connection failed 127.0.0.1:9875','blacklisted socket 127.0.0.1:9875', failmessage);
  SELF.pic := x'01020304';
  SELF.id := if (l.unkname='FRED' AND failcode=4294967293,4294967295,failcode);
  SELF.novalue := 0;
END;

ServiceOutRecord doError2 := TRANSFORM
  SELF.name := 'ERROR: ' + failmessage;
  SELF.pic := x'01020304';
  SELF.id := failcode;
  SELF.novalue := 0;
END;

// Test some failure cases

output(SORT(SOAPCALL(d, 'http://127.0.0.1:9876|http://127.0.0.1:9875','soapbase', { unkname }, DATASET(ServiceOutRecord), onFail(doError(LEFT)),RETRY(0)), record));
output(SORT(SOAPCALL('http://127.0.0.1:9876','soapbase', { string unkname := 'FAIL' }, dataset(ServiceOutRecord),onFail(doError2),RETRY(0)),record));
output(SORT(SOAPCALL(d, 'http://127.0.0.1:9876','soapbaseNOSUCHQUERY', { unkname }, DATASET(ServiceOutRecord), onFail(doError(LEFT)),MERGE(25),RETRY(0)), record));

childRecord := record
unsigned            id;
    end;
    
FullServiceOutRecord := 
    RECORD
        string name;
        data pic;
        unsigned4 id{xpath('r')};
        dataset(childRecord) ids{maxcount(5)};
        unsigned4 novalue;
    END;

//leak children when linked counted rows are enabled, because not all records are read
//Use a count so the results are consistent, and nofold to prevent the code generator removing the child dataset...
output(count(nofold(choosen(SOAPCALL(d, 'http://127.0.0.1:9876','soapbase', { string unkname := d.unkname+'1' }, dataset(FullServiceOutRecord)),1))));
output(count(nofold(choosen(SOAPCALL(d, 'http://127.0.0.1:9876','soapbase', { string unkname := d.unkname+'1' }, dataset(FullServiceOutRecord), merge(3)),2))));

