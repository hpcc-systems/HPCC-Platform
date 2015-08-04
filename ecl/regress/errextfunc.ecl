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

/*
StringLib := SERVICE
 integer StringFilter(STRING, STRING) : eclrtl, library='dab',entrypoint='StringFilter';
END;
*/

STRING StringFilter(STRING s, STRING t) := s + t;
//StringFilter('ab', 'c');

loadxml('<id>4</id>');

#DECLARE (s)
//#IF (StringLib.StringFilter('aabbaa', 'bc') = 'bb')
#IF (StringFilter('aabbaa', 'bc') = 'bb')
//#IF ('b' + 'b' = 'bb')
#SET (s, 10 + 10)
#ELSE
#SET (s, 13 + 13)
#END
export ss := %'s'%;
ss;
