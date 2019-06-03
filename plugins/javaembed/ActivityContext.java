/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems(R).

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

package com.HPCCSystems;

/**
 * This interface may be passed as the first parameter to an embedded function specified with the activity attribute.
 * It allows the activity to determine if it is being executed in a child query, is stranded and other useful information.
*/

public interface ActivityContext
{
    public boolean isLocal();
    public int numSlaves();
    public int numStrands();
    public int querySlave(); // 0 based 0..numSlaves-1
    public int queryStrand(); // 0 based 0..numStrands-1
}