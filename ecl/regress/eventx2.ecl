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

noteReceived(string text) := FUNCTION
  logRecord := { string msg };
  RETURN output(dataset([text], logRecord),NAMED('Received'), extend);
END;

checkComplete() := FUNCTION
  logRecord := { string msg };
  msgs := DATASET(WORKUNIT('Received'),logRecord);
  RETURN IF (count(msgs) = 2, OUTPUT('Dependent Job Completed'));
END;


processReceived(string name) := FUNCTION
  RETURN [noteReceived('Received '+name); checkComplete()];
END;


processReceived('Prerequisite_1') : WHEN('Prerequisite_1', COUNT(1));
processReceived('Prerequisite_2') : WHEN('Prerequisite_2', COUNT(1));
