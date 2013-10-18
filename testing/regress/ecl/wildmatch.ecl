/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

import Std.Str;
IMPORT * FROM lib_unicodelib;

patrec := {STRING10 pat};
strrec := {STRING10 str};
upatrec := {UNICODE10 pat};
ustrrec := {UNICODE10 str};

pats := DATASET([{''}, {'*'}, {'*a'}, {'*a*'}, {'*a*x'}, {'a*'}, {'a*x'}, {'a*x*'}, {'?'}, {'??'}, {'*?'}, {'?*'}, {'?a'}, {'?a*'}, {'?a?'}, {'a?'}, {'a?*'}], patrec);

lowers := DATASET([{''}, {'a'}, {'i'}, {'ta'}, {'yo'}, {'cat'}, {'asp'}, {'spa'}, {'dog'}, {'fax'}, {'axe'}, {'annex'}, {'faxes'}], strrec);

strrec uc(strrec l) := TRANSFORM
    SELF.str := Str.ToUpperCase(l.str);
END;

uppers := PROJECT(lowers, uc(LEFT));

upatrec s2up(patrec l) := TRANSFORM
    SELF.pat := TOUNICODE((DATA)l.pat, 'ascii');
END;

ustrrec s2u(strrec l) := TRANSFORM
    SELF.str := TOUNICODE((DATA)l.str, 'ascii');
END;

upats := PROJECT(pats, s2up(LEFT));
ulowers := PROJECT(lowers, s2u(LEFT));
uuppers := PROJECT(uppers, s2u(LEFT));

lcs := JOIN(pats, lowers, Str.WildMatch(trim(RIGHT.str), trim(LEFT.pat), FALSE), ALL);
ucs := JOIN(pats, uppers, Str.WildMatch(trim(RIGHT.str), trim(LEFT.pat), FALSE), ALL);
uci := JOIN(pats, uppers, Str.WildMatch(trim(RIGHT.str), trim(LEFT.pat), TRUE), ALL);
ulcs := JOIN(upats, ulowers, UnicodeLib.UnicodeWildMatch(RIGHT.str, LEFT.pat, FALSE), ALL);
uucs := JOIN(upats, uuppers, UnicodeLib.UnicodeWildMatch(RIGHT.str, LEFT.pat, FALSE), ALL);
uuci := JOIN(upats, uuppers, UnicodeLib.UnicodeWildMatch(RIGHT.str, LEFT.pat, TRUE), ALL);

OUTPUT(lcs, NAMED('lower_case_sensitive'));
OUTPUT(ucs, NAMED('upper_case_sensitive'));
OUTPUT(uci, NAMED('upper_case_insensitive'));
OUTPUT(ulcs, NAMED('unicode_lower_case_sensitive'));
OUTPUT(uucs, NAMED('unicode_upper_case_sensitive'));
OUTPUT(uuci, NAMED('unicode_upper_case_insensitive'));
