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

r := record
  string line;
  end;

d := dataset([
{'Gavin Hawthorn'},
{'Gavin C Hawthorn'},
{'John Holt'},
{'George W Bush'},
{''}],r);

pattern ws := [' ','\t',',']+;
pattern id := PATTERN('[a-zA-Z]+');


//-----------------

pattern twoNames := id ws id;

firstLastRecord :=
        record
string      firstName;
string      lastName;
        end;

firstLastRecord extractFirstLast() :=
    TRANSFORM
        SELF.firstName := MATCHTEXT(id[1]);
        SELF.lastName := MATCHTEXT(id[2]);
    END;

//------------------

pattern threeNames := id ws id ws id;

firstMidLastRecord :=
        record
string      firstName;
string      middleName;
string      lastName;
        end;

firstMidLastRecord extractFirstMidLast() :=
    TRANSFORM
        SELF.firstName := MATCHTEXT(id[1]);
        SELF.middleName := MATCHTEXT(id[2]);
        SELF.lastName := MATCHTEXT(id[3]);
    END;


//------------------ First macro that adds the fields into the record ------------

applyPatternParse(in, extractTransform, extractPattern, outFile) := macro

#uniquename (combinedRec)
#uniquename (combinedTransform)

%combinedRec% := record
recordof(in);
recordof(extractTransform());
                end;

%combinedRec% %combinedTransform%(d l) := TRANSFORM
    SELF := l;
    SELF := ROW(extractTransform());
    END;


outFile := parse(in, line, extractPattern, %combinedTransform%(LEFT), ALL, WHOLE, BEST, MANY);

endmacro;

//-----------------------

applyPatternParse(d, extractFirstLast, twoNames, out);
output (out);

applyPatternParse(d, extractFirstMidLast, threeNames, out2);
output (out2);
