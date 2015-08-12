/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

#IF (1.1D)
OUTPUT('success');
#else
OUTPUT('failure');
#END

#IF (0.0D)
OUTPUT('failure');
#else
OUTPUT('success');
#END

#IF ((boolean)1.1D)
OUTPUT('success');
#else
OUTPUT('failure');
#END

#IF ((boolean)0.0D)
OUTPUT('failure');
#else
OUTPUT('success');
#END

decimal10_2 zero := 0.0D : stored('zero');
decimal10_2 one := 1.0D : stored('one');

output((boolean)zero);
output((boolean)one);

