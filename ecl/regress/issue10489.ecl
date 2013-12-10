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

LayoutScoredFetch := RECORD
  UNSIGNED8 F1;
END;

 OutputLayout_Base := RECORD
  BOOLEAN Resolved := FALSE;
  DATASET(LayoutScoredFetch) Results;
END;

OutputLayout_Batch := RECORD(OutputLayout_Base)
  UNSIGNED8 Reference;
END;

ds := DATASET('x',OutputLayout_Batch,thor);

ScoreSummary(DATASET(OutputLayout_Base) ds0) :=
        PROJECT(ds0(EXISTS(Results)),TRANSFORM(LayoutScoredFetch,SELF := LEFT.Results[1]));

Stats := ScoreSummary(ds);
Stats;
