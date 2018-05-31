/*##############################################################################

    Copyright (C) 2012 HPCC Systems®.

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

// The aim of this code is to test the wildcard spray from an empty and a not empty directory
// and test:
// 1. No failure if empty directory sprayed with wildcard
// 2. 'failifnosourcefile' feature works as it suppose to be
// 
// When an empty directory sprayed with wildcard and 'failifnosourcefile' is enabled then 
// it forces the DFU server to throw exeption.
//
// NOTHOR() effectively forces something to be executed globally.  At the moment if a global operation
// fails then the query fails - rather than continuing and only failing if the result of using that
// operation causes a failure.
// So, to avoid to abort this code is excluded from Thor target
//nothor

//nohthor
//class=spray


import std.system.thorlib;
import Std.File AS FileServices;
import ^ as root;

engine := thorlib.platform();
prefix := '~regress::' + engine + '-';
suffix := '-' + WORKUNIT;

dropzonePath := '/var/lib/HPCCSystems/mydropzone/' : STORED('dropzonePath');

unsigned VERBOSE := 0;

Layout_Person := RECORD
  STRING3  name;
  UNSIGNED2 age;
  BOOLEAN good;
END;

empty := DATASET([], Layout_Person);

allPeople := DATASET([ {'foo', 10, 1},
                       {'bar', 12, 0},
                       {'baz', 32, 1}]
            ,Layout_Person);

sprayPrepFileName := prefix + 'spray_prep' + suffix;
sprayOutFileName := prefix + 'spray_test' + suffix;
dsSetup := allPeople;
emptyDirPath := dropzonePath+'empty';
notEmptyDirPath := dropzonePath+'notempty';

// Create an 'empty' directory
FileServices.CreateExternalDirectory('.', emptyDirPath);


// Create a 'notempty' directory
FileServices.CreateExternalDirectory('.', notEmptyDirPath);

//  Create a small logical file
setupFile := output(dsSetup, , DYNAMIC(sprayPrepFileName), CSV, OVERWRITE);

rec := RECORD
  string result;
  string msg;
end;

desprayOutFileName := notEmptyDirPath + '/'+ 'spray_input' + suffix;

// Despray it to 'notempty' dir in default drop zone
rec despray(rec l) := TRANSFORM
  SELF.msg := FileServices.fDespray(
                       LOGICALNAME := sprayPrepFileName
                      ,DESTINATIONIP := '.'
                      ,DESTINATIONPATH := desprayOutFileName
                      ,ALLOWOVERWRITE := True
                      );
  SELF.result := 'Despray Pass';
end;

dst1 := NOFOLD(DATASET([{'', ''}], rec));
p1 := NOTHOR(PROJECT(NOFOLD(dst1), despray(LEFT)));
c1 := CATCH(NOFOLD(p1), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Despray Fail',
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    desprayOut := output(c1);
#else
    desprayOut := output(c1, {result});
#end


sprayrec := RECORD
  string sourcepath;
  boolean failifnosourcefile;
  string result;
  string msg;
end;


// Try to spray from an empty directory with default failifnosourcefile (=0) 

sprayrec spray(sprayrec l) := TRANSFORM
    SELF.msg := FileServices.fSprayVariable(
                        SOURCEIP := '.',
                        SOURCEPATH := l.sourcepath,
                        //RECORDSIZE := RecordSize,
                        DESTINATIONGROUP := 'mythor',
                        DESTINATIONLOGICALNAME := sprayOutFileName,
                        TIMEOUT := -1,
                        ESPSERVERIPPORT := 'http://127.0.0.1:8010/FileSpray',
                        ALLOWOVERWRITE := true,
                        FAILIFNOSOURCEFILE := l.failifnosourcefile
                        );
    self.sourcepath := l.sourcepath;
    self.failifnosourcefile := l.failifnosourcefile;
    self.result := l.result;
end;


dst2 := NOFOLD(DATASET([{emptyDirPath+'/*', false, 'Pass', ''}], sprayrec));
p2 := PROJECT(NOFOLD(dst2), spray(LEFT));
c2 := CATCH(NOFOLD(p2), ONFAIL(TRANSFORM(sprayrec,
                                SELF.sourcepath := emptyDirPath+'/*',
                                SELF.failifnosourcefile := false,
                                SELF.result := 'Fail',
                                SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    sprayOut1 := output(c2);
#else
    sprayOut1 := output(c2, {result}, NAMED('Empty_Dir_No_FailIFNoSourceFile'));
#end


// Try to spray from an empty directory with failifnosourcefile=1 
// It should always fail so if it pass something broken

dst3 := NOFOLD(DATASET([{emptyDirPath+'/*', true, 'Fail', ''}], sprayrec));
p3 := PROJECT(NOFOLD(dst3), spray(LEFT));
c3 := CATCH(NOFOLD(p3), ONFAIL(TRANSFORM(sprayrec,
                                 SELF.sourcepath := emptyDirPath+'/*',
                                 SELF.failifnosourcefile := true,
                                 SELF.result := 'Pass',
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    sprayOut2 := output(c3);
#else
    sprayOut2 := output(c3, {result}, NAMED('Empty_Dir_FailIFNoSourceFile'));
#end


// Try to spray from a not empty directory with default failifnosourcefile (=0) 

dst4 := NOFOLD(DATASET([{notEmptyDirPath+'/*', false, 'Pass', ''}], sprayrec));
p4 := PROJECT(NOFOLD(dst4), spray(LEFT));
c4 := CATCH(NOFOLD(p4), ONFAIL(TRANSFORM(sprayrec,
                                 SELF.sourcepath := notEmptyDirPath+'/*',
                                 SELF.failifnosourcefile := false,
                                 SELF.result := 'Fail',
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    sprayOut3 := output(c4);
#else
    sprayOut3 := output(c4, {result}, NAMED('Not_Empty_Dir_No_FailIFNoSourceFile'));
#end


// Try to spray from a not empty directory with failifnosourcefile=1 

dst5 := NOFOLD(DATASET([{notEmptyDirPath+'/*', true, 'Pass', ''}], sprayrec));
p5 := PROJECT(NOFOLD(dst5), spray(LEFT));
c5 := CATCH(NOFOLD(p5), ONFAIL(TRANSFORM(sprayrec,
                                 SELF.sourcepath := notEmptyDirPath+'/*',
                                 SELF.failifnosourcefile := true,                                 
                                 SELF.result := 'Fail',
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    sprayOut4 := output(c5);
#else
    sprayOut4 := output(c5, {result}, NAMED('Not_Empty_Dir_FailIFNoSourceFile'));
#end


SEQUENTIAL(
    setupFile,
    desprayOut,
    sprayOut1,
    sprayOut2,
    sprayOut3,
    sprayOut4,

    // Clean-up
    FileServices.DeleteExternalFile('.', desprayOutFileName),
    FileServices.DeleteExternalFile('.', emptyDirPath),
    FileServices.DeleteExternalFile('.', notEmptyDirPath),
    FileServices.DeleteLogicalFile(sprayOutFileName),
    FileServices.DeleteLogicalFile(sprayPrepFileName)
);
