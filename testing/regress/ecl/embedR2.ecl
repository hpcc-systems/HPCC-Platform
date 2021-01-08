/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

//class=embedded
//class=embedded-r
//class=3rdparty

IMPORT R;
 
recStruct := RECORD
  INTEGER b;
  INTEGER c;
END;

// Test a nested record, a field of type record, and some nested datasets

r_rec := RECORD
   integer a , record integer aa end, set of integer s, recStruct rs ,DATASET(recStruct) B, DATASET(recStruct) C
END;

// Return them from R

r_rec fred() := EMBED( R )
   indata <- list(a = 2, aa = 3, s = c(1,2,3), rs=list(B=12, C=34), B = data.frame(10:20,100:110), C = data.frame(20:30,200:210), D=list(B=12,C=34))
   # print(str(indata)) # handy for debugging if things go wrong
   indata
ENDEMBED;

r_rec george(r_rec indata) := EMBED( R )
   # print(str(indata)) # should look similar to previous
   indata
ENDEMBED;
  
output(george(fred()));
 

