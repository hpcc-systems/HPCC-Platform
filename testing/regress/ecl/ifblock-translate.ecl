/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

// No point running on multiple engines - this is testing eclrtl

//nothor
//nohthor

IMPORT std;

STRING uc(STRING instr) := Std.Str.ToUpperCase(instr);

rAA := RECORD
    string002        SegmentID;
    string012        FileSeqNum;
  string50 aa1;
END;

rZZ := RECORD
    string002        SegmentID;
    string012        FileSeqNum;
  string50 zz1;
END;

rAB := RECORD
    string002        SegmentID;
    string012        FileSeqNum;
  string50 ab1;
END;

rAD := RECORD
    string002        SegmentID;
    string012        FileSeqNum;
  string50 ad1;
END;

rIS := RECORD
    string002        SegmentID;
    string012        FileSeqNum;
  string50 is1;
END;

rin := RECORD
    string002        SegmentID;
    string012        FileSeqNum;
    IFBLOCK(uc(self.SegmentId) = 'AA')
        rAA AND NOT [SegmentId, FileSeqNum] aa;
    END;
    IFBLOCK(uc(self.SegmentId) = 'ZZ')
        rZZ AND NOT [SegmentId, FileSeqNum] zz;
    END;
    IFBLOCK(uc(self.SegmentId) = 'AB')
        rAB AND NOT [SegmentId, FileSeqNum] ab;
    END;
    IFBLOCK(uc(self.SegmentId) = 'AD')
        rAD AND NOT [SegmentId, FileSeqNum] ad;
    END;
    IFBLOCK(uc(self.SegmentId) = 'IS')
        rIS AND NOT [SegmentId, FileSeqNum] is;
    END;
END;

rout := RECORD
    string        SegmentID;
END;

s := SERVICE
   streamed dataset(rout) stransform(streamed dataset input) : eclrtl,pure,library='eclrtl',entrypoint='transformRecord',passParameterMeta(true);
END;

d := DATASET([{'AA', 'ABC', 'AAstring' }], rin);

output(s.stransform(d));  
