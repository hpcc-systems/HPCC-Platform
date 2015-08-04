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

CatRecord := RECORD
    STRING sCats;
    UNSIGNED julianDays;
END;

xRecord := RECORD
    STRING s1;
    STRING s2;
    DATASET(catRecord) y;
END;

absBase := MODULE,VIRTUAL
export a := 1;
END;

base := MODULE(absBase)

 EXPORT f(DATASET(CatRecord) dsPrevCats, xRecord x, UNSIGNED nNumDays) := FUNCTION

    sCurrCat := x.s1;

    matchRec := RECORD
        BOOLEAN bMatch := FALSE;
    END;

    matchRec isMatch(CatRecord recPrevCat, STRING50 sCat, UNSIGNED nNumDays, UNSIGNED jCurrDays) := TRANSFORM

        STRING100 sCurrWord := REGEXREPLACE(',', '-' + TRIM(sCat) + '-', '-,-');
        STRING100 sCurrCatA := REGEXREPLACE(',', sCurrWord, '|');
        STRING100 sCurrCat := REGEXREPLACE('(\\|)$', TRIM(sCurrCatA), '');

        STRING100 sPrevWord := REGEXREPLACE(',', '-' + TRIM(recPrevCat.sCats) + '-', '-,-');
        STRING100 sPrevCatA := REGEXREPLACE(',', sPrevWord, '|');
        STRING100 sPrevCat := REGEXREPLACE('(\\|)$', TRIM(sPrevCatA), '');

        SELF.bMatch := IF( (LENGTH(TRIM(recPrevCat.sCats)) > 0 AND LENGTH(TRIM(sCat)) > 0) AND ((recPrevCat.JulianDays - jCurrDays) <= nNumDays), REGEXFIND(TRIM(sPrevCat), TRIM(sCurrCat)), FALSE);
    END;

    dsFound := PROJECT(dsPrevCats, isMatch(LEFT, sCurrCat, nNumDays, 12345678));

    RETURN COUNT(dsFound(bMatch = TRUE)) > 0;
END;
END;


infile := dataset('x', xRecord, thor);

t := TABLE(infile, { base.f(y, ROW(infile), 23); });

output(t);
