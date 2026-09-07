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
##############################################################################*/

// Verifies the parallel read-ahead disk I/O paths introduced for flat,
// compressed, CSV, and XML file reads.
//
// Each file is large enough (> 6 MB per part) to cross all four 1 MB
// ring-buffer chunk boundaries of the default parallel read-ahead stream.
//
// Row content is deterministic: val = (seq * 997) % 65521.
// After each read-back, two invariants are checked:
//   cnt_diff = COUNT(read_back) - N  -- 0 means no rows lost at boundaries
//   bad_rows = rows where val != expected  -- 0 means no data corruption

//class=file
//nohthor
//noroxie

IMPORT Std.File;

prefix := '~regress::mixed_compression_diskread::';

Rec := RECORD
    UNSIGNED4 seq;
    UNSIGNED4 val;    // val = (seq * 997) % 65521
    UNSIGNED4 val2;   // independent check: val2 = (seq * 31337) % 65521
END;

// 800 000 rows × 12 bytes = 9.6 MB per part; crosses all 4 × 1 MB ring-buffer
// chunks multiple times, and exercises row-boundary alignment at each crossing.
N := 800000;

Rec makeRow(UNSIGNED4 s) := TRANSFORM
    SELF.seq  := s;
    SELF.val  := (s * 997)   % 65521;
    SELF.val2 := (s * 31337) % 65521;
END;

ds := DATASET(N, makeRow(COUNTER));

// Returns {cnt_diff=0, bad_rows=0} if d has correct count and all values match.
checkRec := RECORD
    INTEGER8 cnt_diff;   // COUNT(d) - N; 0 = correct row count
    INTEGER8 bad_rows;   // rows where either val is wrong; 0 = no corruption
END;

verify(DATASET(Rec) d) := FUNCTION
    cnt := COUNT(d);
    bad := COUNT(d(val  != (seq * 997)   % 65521 OR
                  val2 != (seq * 31337) % 65521));
    RETURN ROW({cnt - N, bad}, checkRec);
END;

// Dataset declarations (lazy; reads happen only inside SEQUENTIAL after writes).
flatDs    := DATASET(prefix + 'flat',     Rec, FLAT);
compDs    := DATASET(prefix + 'comp',     Rec, FLAT);
csvDs     := DATASET(prefix + 'csv',      Rec, CSV);
csvCDs    := DATASET(prefix + 'csv_comp', Rec, CSV);
xmlDs     := DATASET(prefix + 'xml',      Rec, XML('Dataset/Row'));
xmlCDs    := DATASET(prefix + 'xml_comp', Rec, XML('Dataset/Row'));

SEQUENTIAL(
    // Write all six formats in parallel
    PARALLEL(
        OUTPUT(ds,, prefix + 'flat',     OVERWRITE),
        OUTPUT(ds,, prefix + 'comp',     OVERWRITE, COMPRESSED),
        OUTPUT(ds,, prefix + 'csv',      CSV, OVERWRITE),
        OUTPUT(ds,, prefix + 'csv_comp', CSV, OVERWRITE, COMPRESSED),
        OUTPUT(ds,, prefix + 'xml',      XML, OVERWRITE),
        OUTPUT(ds,, prefix + 'xml_comp', XML, OVERWRITE, COMPRESSED)
    ),
    PARALLEL(
        // Verify each format; all six should produce {cnt_diff=0, bad_rows=0}
        OUTPUT(verify(flatDs),  NAMED('flat')),
        OUTPUT(verify(compDs),  NAMED('comp')),
        OUTPUT(verify(csvDs),   NAMED('csv')),
        OUTPUT(verify(csvCDs),  NAMED('csv_comp')),
        OUTPUT(verify(xmlDs),   NAMED('xml')),
        OUTPUT(verify(xmlCDs),  NAMED('xml_comp')),
    ),
    // Clean up
    PARALLEL(
        File.DeleteLogicalFile(prefix + 'flat'),
        File.DeleteLogicalFile(prefix + 'comp'),
        File.DeleteLogicalFile(prefix + 'csv'),
        File.DeleteLogicalFile(prefix + 'csv_comp'),
        File.DeleteLogicalFile(prefix + 'xml'),
        File.DeleteLogicalFile(prefix + 'xml_comp')
    )
);
