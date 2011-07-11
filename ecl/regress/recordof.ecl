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

r := record
  string line;
  end;

d := dataset([
{'Gavin Halliday'},
{'Gavin C Halliday'},
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
