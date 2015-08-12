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

// output is u'', expected u'startend'

unicode s := u'start' : stored('s');
unicode m := u'' : stored('m');
unicode e := u'end' : stored('e');

output(s + m + e, named('unicode_empty_string'));
//e := u'end' : stored('e');
//output(u'start' + u'' + u'end', named('unicode_empty_string'));



