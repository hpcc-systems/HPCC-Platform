/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

//nothor
//noroxie

import $.setup;
prefix := setup.Files(false, false).QueryFilePrefix;

IMPORT Std;

// Test data record structure
TestRec := RECORD
    UNSIGNED4 id;
    STRING100 name;
    STRING200 padding;
END;

// Create test data with varying sizes
createTestData(UNSIGNED4 numRecords) := DATASET(numRecords, TRANSFORM(TestRec,
    SELF.id := COUNTER,
    SELF.name := 'Test record number ' + (STRING)COUNTER,
    SELF.padding := 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
));

// Setup: Create test files with various properties
setupFiles := SEQUENTIAL(
    OUTPUT('=== Creating Test Files ==='),

    Std.File.DeleteSuperFile(prefix + 'super1'),

    // Small files with varying record counts
    OUTPUT(createTestData(100), , prefix + 'file1', EXPIRE(1), OVERWRITE),
    OUTPUT(createTestData(200), , prefix + 'file2', EXPIRE(1), OVERWRITE),
    OUTPUT(createTestData(500), , prefix + 'file3', EXPIRE(1), OVERWRITE),

    // Medium record count files
    OUTPUT(createTestData(1000), , prefix + 'file4', EXPIRE(1), OVERWRITE),
    OUTPUT(createTestData(2000), , prefix + 'file5', EXPIRE(1), OVERWRITE),

    // Test-prefixed files for pattern matching
    OUTPUT(createTestData(300), , prefix + 'testfile1', EXPIRE(1), OVERWRITE),
    OUTPUT(createTestData(400), , prefix + 'testfile2', EXPIRE(1), OVERWRITE),

    // Create a superfile for testing
    Std.File.CreateSuperFile(prefix + 'super1'),
    Std.File.AddSuperFile(prefix + 'super1', prefix + 'file1'),
    Std.File.AddSuperFile(prefix + 'super1', prefix + 'file2'),

    OUTPUT('=== Test Files Created ===')
);

// Cleanup: Delete test files
cleanupFiles := SEQUENTIAL(
    OUTPUT('=== Cleaning Up Test Files ==='),

    Std.File.DeleteSuperFile(prefix + 'super1'),
    Std.File.DeleteLogicalFile(prefix + 'file1', true),
    Std.File.DeleteLogicalFile(prefix + 'file2', true),
    Std.File.DeleteLogicalFile(prefix + 'file3', true),
    Std.File.DeleteLogicalFile(prefix + 'file4', true),
    Std.File.DeleteLogicalFile(prefix + 'file5', true),
    Std.File.DeleteLogicalFile(prefix + 'testfile1', true),
    Std.File.DeleteLogicalFile(prefix + 'testfile2', true),

    Std.File.DeleteSuperFile(prefix + 'super1'),

    OUTPUT('=== Cleanup Complete ===')
);

// Test 1: Basic usage - list files in our test scope
test1 := MODULE
    EXPORT result := Std.File.LogicalFileListFiltered(prefix + '*');
    EXPORT name := 'Test 1: Basic usage (all test files)';
    EXPORT verify := SEQUENTIAL(
        OUTPUT(result.count, NAMED('Test1_Count')),
        OUTPUT(result.limitBreached, NAMED('Test1_LimitBreached')),
        OUTPUT(COUNT(result.files), NAMED('Test1_FileCount'))
    );
END;

// Test 2: Usage with explicit limit
test2 := MODULE
    EXPORT result := Std.File.LogicalFileListFiltered(prefix + '*', maxFileLimit := 5);
    EXPORT name := 'Test 2: With file limit and breach detection';
    EXPORT verify := SEQUENTIAL(
        OUTPUT(result.count, NAMED('Test2_Count')),
        OUTPUT(result.limitBreached, NAMED('Test2_LimitBreached')),
        OUTPUT(COUNT(result.files) <= 5, NAMED('Test2_LimitRespected')),
        OUTPUT(COUNT(result.files), NAMED('Test2_ActualFileCount'))
    );
END;

// Test 3: Pattern matching
test3 := MODULE
    EXPORT result := Std.File.LogicalFileListFiltered(prefix + 'testfile*');
    EXPORT name := 'Test 3: Pattern matching (testfile*)';
    EXPORT verify := SEQUENTIAL(
        OUTPUT(result.count, NAMED('Test3_Count')),
        OUTPUT(COUNT(result.files), NAMED('Test3_FileCount'))
    );
END;

// Test 4: SuperFiles only
test4 := MODULE
    EXPORT filters := 'is:superfile';
    EXPORT result := Std.File.LogicalFileListFiltered(prefix + '*', filters := filters);
    EXPORT name := 'Test 4: SuperFiles only';
    EXPORT verify := SEQUENTIAL(
        OUTPUT(result.count, NAMED('Test4_Count')),
        OUTPUT(COUNT(result.files), NAMED('Test4_FileCount'))
    );
END;

// Test 5: Normal files only (not superfiles)
test5 := MODULE
    EXPORT filters := 'is:normal';
    EXPORT result := Std.File.LogicalFileListFiltered(prefix + '*', filters := filters);
    EXPORT name := 'Test 5: Normal files only (not superfiles)';
    EXPORT verify := SEQUENTIAL(
        OUTPUT(result.count, NAMED('Test5_Count')),
        OUTPUT(COUNT(result.files), NAMED('Test5_FileCount'))
    );
END;

// Test 6: Filter - record count range (should exclude file4=1000 and file5=2000)
test6 := MODULE
    EXPORT filters := 'rowcount>=100,rowcount<1000';
    EXPORT result := Std.File.LogicalFileListFiltered(prefix + '*', filters := filters);
    EXPORT name := 'Test 6: Record count range (100 <= rowcount < 1000)';
    EXPORT verify := SEQUENTIAL(
        OUTPUT(result.count, NAMED('Test6_Count')),
        OUTPUT(result.count = 6, NAMED('Test6_ExpectedCount')), // Should match: file1(100), file2(200), file3(500), testfile1(300), testfile2(400), super1(300)
        OUTPUT(COUNT(result.files), NAMED('Test6_FileCount'))
    );
END;

// Test 7: Combined filters - record count with normal files (excludes super1 and large files)
test7 := MODULE
    EXPORT filters := 'rowcount>=200,rowcount<=500,is:normal';
    EXPORT result := Std.File.LogicalFileListFiltered(prefix + '*', filters := filters);
    EXPORT name := 'Test 7: Combined filters (200 <= rowcount <= 500 AND normal files)';
    EXPORT verify := SEQUENTIAL(
        OUTPUT(result.count, NAMED('Test7_Count')),
        OUTPUT(result.count = 4, NAMED('Test7_ExpectedCount')), // Should match: file2(200), file3(500), testfile1(300), testfile2(400) = 4, but super1 excluded
        OUTPUT(COUNT(result.files), NAMED('Test7_FileCount'))
    );
END;

// Test 8: Filter - date range excludes all recent files (proves date filtering works)
test8 := MODULE
    EXPORT filters := 'modified<=' + Std.Date.DateToString(Std.Date.Today() - 1, '%Y-%m-%d');
    EXPORT result := Std.File.LogicalFileListFiltered(prefix + '*', filters := filters);
    EXPORT name := 'Test 8: Modified <= <YESTERDAY> (should exclude all test files)';
    EXPORT verify := SEQUENTIAL(
        OUTPUT(result.count, NAMED('Test8_Count')),
        OUTPUT(result.count = 0, NAMED('Test8_ExpectedZero')), // All test files created before yesterday, so none match
        OUTPUT(COUNT(result.files), NAMED('Test8_FileCount'))
    );
END;

// Test 9: Filter - files NOT in superfiles (should exclude file1 and file2 which are in super1)
test9 := MODULE
    EXPORT filters := '!has:SuperOwners';
    EXPORT result := Std.File.LogicalFileListFiltered(prefix + '*', filters := filters);
    EXPORT name := 'Test 9: Files not in superfiles (!has:SuperOwners)';
    EXPORT verify := SEQUENTIAL(
        OUTPUT(result.count, NAMED('Test9_Count')),
        OUTPUT(result.count = 6, NAMED('Test9_ExpectedCount')), // Should be 6: file3,file4,file5,testfile1,testfile2,super1 (excludes file1,file2)
        OUTPUT(COUNT(result.files), NAMED('Test9_FileCount'))
    );
END;

// Test 10: Empty fields parameter returns default 7 fields
test10 := MODULE
    EXPORT result := Std.File.LogicalFileListFiltered(prefix + '*', fields := '');
    EXPORT name := 'Test 10: Default fields (backward compatible)';
    // Strip prefix from names and remove modified/cluster fields to avoid variability
    EXPORT filesNormalized := TABLE(result.files, {
        STRING name := name[LENGTH(prefix)..];
        UNSIGNED8 size := size;
        UNSIGNED8 rowcount := rowcount;
        BOOLEAN superfile := superfile;
    });
    EXPORT verify := SEQUENTIAL(
        OUTPUT(result.count, NAMED('Test10_Count')),
        OUTPUT(result.count = 8, NAMED('Test10_AllFilesReturned')),
        OUTPUT(filesNormalized, NAMED('Test10_Files'))
    );
END;

// Test 11: Minimal fields - name is mandatory even if not specified
test11 := MODULE
    EXPORT fields := 'size';
    EXPORT result := Std.File.LogicalFileListFiltered(prefix + '*', fields := fields);
    EXPORT name := 'Test 11: Name field auto-added (requested only size)';
    // Strip prefix from names and remove modified/cluster fields to avoid variability
    EXPORT filesNormalized := TABLE(result.files, {
        STRING name := name[LENGTH(prefix)..];
        UNSIGNED8 size := size;
    });
    EXPORT verify := SEQUENTIAL(
        OUTPUT(result.count, NAMED('Test11_Count')),
        OUTPUT(result.count = 8, NAMED('Test11_AllFilesReturned')),
        OUTPUT(filesNormalized, NAMED('Test11_Files'))
    );
END;

// Test 12: Combine custom fields with filters (tests both features together)
test12 := MODULE
    EXPORT fields := 'name,superfile,size';
    EXPORT filters := 'is:normal,rowcount>=200';
    EXPORT result := Std.File.LogicalFileListFiltered(prefix + '*', filters := filters, fields := fields);
    EXPORT name := 'Test 12: Custom fields + filters combined';
    // Strip prefix from names and remove modified/cluster fields to avoid variability
    EXPORT filesNormalized := TABLE(result.files, {
        STRING name := name[LENGTH(prefix)..];
        BOOLEAN superfile := superfile;
        UNSIGNED8 size := size;
    });
    EXPORT verify := SEQUENTIAL(
        OUTPUT(result.count, NAMED('Test12_Count')),
        OUTPUT(result.count = 6, NAMED('Test12_ExpectedCount')), // file2(200), file3(500), file4(1000), file5(2000), testfile1(300), testfile2(400) - all normal files >= 200
        OUTPUT(filesNormalized, NAMED('Test12_Files'))
    );
END;

// Run all tests with setup and cleanup
SEQUENTIAL(
    setupFiles,

    OUTPUT(test1.name),
    test1.verify,

    OUTPUT(test2.name),
    test2.verify,

    OUTPUT(test3.name),
    test3.verify,

    OUTPUT(test4.name),
    test4.verify,

    OUTPUT(test5.name),
    test5.verify,

    OUTPUT(test6.name),
    test6.verify,

    OUTPUT(test7.name),
    test7.verify,

    OUTPUT(test8.name),
    test8.verify,

    OUTPUT(test9.name),
    test9.verify,

    OUTPUT(test10.name),
    test10.verify,

    OUTPUT(test11.name),
    test11.verify,

    OUTPUT(test12.name),
    test12.verify,

    OUTPUT('=== Test Suite Complete ==='),

    cleanupFiles
);
