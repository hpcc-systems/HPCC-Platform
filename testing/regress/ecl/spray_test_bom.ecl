/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

//class=spray
//nohthor
//noroxie

import Std.File AS FileServices;
import Std.Str AS StringServices;
import $.setup;

OriginalTextFilesIp :=  '.';
ClusterName := 'mythor';
ESPportIP := FileServices.GetEspURL() + '/FileSpray';

prefix := setup.Files(false, false).QueryFilePrefix;

dropzonePath := FileServices.GetDefaultDropZone() : STORED('dropzonePath');

unsigned VERBOSE := 0;
unsigned CLEANUP := 1;

jsonOutRec := RECORD
    DATA1 myData;
END;

jsonDs := DATASET([
  {x'ef'}, {x'bb'}, {x'bf'}, {x'7b'}, {x'22'}, {x'74'}, {x'65'}, {x'73'}, {x'74'}, {x'69'}, {x'6e'}, {x'67'}, {x'22'}, {x'3a'}, {x'20'}, {x'5b'},     //  |...{"testing": [|
  {x'31'}, {x'2c'}, {x'32'}, {x'2c'}, {x'33'}, {x'5d'}, {x'7d'}, {x'0a'}                                                                                                                 //  |1,2,3]}.|
               ], jsonOutRec);

// To avoid the problem with an upper case letter 'W' (from the WORKUNIT) in the logical file name
JsonSourceFileName := StringServices.ToLowerCase(WORKUNIT) + '-input_with_bom.json';
JsonSourceFile := dropzonePath + '/' + JsonSourceFileName;

xmlOutRec := RECORD
    DATA1 myData;
END;

xmlDs := DATASET([
    {x'ef'}, {x'bb'}, {x'bf'}, {x'3c'}, {x'52'}, {x'6f'},     //  |...<Ro|
    {x'77'}, {x'74'}, {x'61'}, {x'67'}, {x'3e'}, {x'3c'}, {x'6e'}, {x'61'}, {x'6d'}, {x'65'}, {x'3e'}, {x'66'}, {x'6f'}, {x'6f'}, {x'3c'}, {x'2f'},     //  |wtag><name>foo</|
    {x'6e'}, {x'61'}, {x'6d'}, {x'65'}, {x'3e'}, {x'3c'}, {x'61'}, {x'67'}, {x'65'}, {x'3e'}, {x'31'}, {x'30'}, {x'3c'}, {x'2f'}, {x'61'}, {x'67'},     //  |name><age>10</ag|
    {x'65'}, {x'3e'}, {x'3c'}, {x'67'}, {x'6f'}, {x'6f'}, {x'64'}, {x'3e'}, {x'74'}, {x'72'}, {x'75'}, {x'65'}, {x'3c'}, {x'2f'}, {x'67'}, {x'6f'},     //  |e><good>true</go|
    {x'6f'}, {x'64'}, {x'3e'}, {x'3c'}, {x'2f'}, {x'52'}, {x'6f'}, {x'77'}, {x'74'}, {x'61'}, {x'67'}, {x'3e'}, {x'0a'}, {x'3c'}, {x'52'}, {x'6f'},     //  |od></Rowtag>.<Ro|
    {x'77'}, {x'74'}, {x'61'}, {x'67'}, {x'3e'}, {x'3c'}, {x'6e'}, {x'61'}, {x'6d'}, {x'65'}, {x'3e'}, {x'62'}, {x'61'}, {x'72'}, {x'3c'}, {x'2f'}, //  |wtag><name>bar</|
    {x'6e'}, {x'61'}, {x'6d'}, {x'65'}, {x'3e'}, {x'3c'}, {x'61'}, {x'67'}, {x'65'}, {x'3e'}, {x'31'}, {x'32'}, {x'3c'}, {x'2f'}, {x'61'}, {x'67'}, //  |name><age>12</ag|
    {x'65'}, {x'3e'}, {x'3c'}, {x'67'}, {x'6f'}, {x'6f'}, {x'64'}, {x'3e'}, {x'66'}, {x'61'}, {x'6c'}, {x'73'}, {x'65'}, {x'3c'}, {x'2f'}, {x'67'}, //  |e><good>false</g|
    {x'6f'}, {x'6f'}, {x'64'}, {x'3e'}, {x'3c'}, {x'2f'}, {x'52'}, {x'6f'}, {x'77'}, {x'74'}, {x'61'}, {x'67'}, {x'3e'}, {x'0a'}, {x'3c'}, {x'52'}, //  |ood></Rowtag>.<R|
    {x'6f'}, {x'77'}, {x'74'}, {x'61'}, {x'67'}, {x'3e'}, {x'3c'}, {x'6e'}, {x'61'}, {x'6d'}, {x'65'}, {x'3e'}, {x'62'}, {x'61'}, {x'7a'}, {x'3c'}, // |owtag><name>baz<|
    {x'2f'}, {x'6e'}, {x'61'}, {x'6d'}, {x'65'}, {x'3e'}, {x'3c'}, {x'61'}, {x'67'}, {x'65'}, {x'3e'}, {x'33'}, {x'32'}, {x'3c'}, {x'2f'}, {x'61'}, //  |/name><age>32</a|
    {x'67'}, {x'65'}, {x'3e'}, {x'3c'}, {x'67'}, {x'6f'}, {x'6f'}, {x'64'}, {x'3e'}, {x'74'}, {x'72'}, {x'75'}, {x'65'}, {x'3c'}, {x'2f'}, {x'67'}, // |ge><good>true</g|
    {x'6f'}, {x'6f'}, {x'64'}, {x'3e'}, {x'3c'}, {x'2f'}, {x'52'}, {x'6f'}, {x'77'}, {x'74'}, {x'61'}, {x'67'}, {x'3e'}, {x'0a'}                                //  |ood></Rowtag>./|
               ], xmlOutRec);

// To avoid the problem with an upper case letter 'W' (from the WORKUNIT) in the logical file name
XmlSourceFileName := StringServices.ToLowerCase(WORKUNIT) + '-input_with_bom.xml';
XMLSourceFile := dropzonePath + '/' + XmlSourceFileName;


jsonInRec := RECORD
    set of integer testing{xpath('/testing')};
END;

DestFile1 := prefix + 'with-bom.json';
ds1 := DATASET(DestFile1, jsonInRec, JSON('/', NOROOT));

xmlInRec := RECORD
    STRING  name;
    UNSIGNED age;
    BOOLEAN good;
END;

DestFile2 := prefix + 'with-bom.xml';
ds2:= DATASET(DestFile2, xmlInRec, XML('Rowtag', NOROOT));

SEQUENTIAL (
    #if (VERBOSE = 1)
    output(OriginalTextFilesIp, NAMED('OriginalTextFilesIp'));
    output(SourceFileName, NAMED('SourceFileName'));
    output(SourceFile, NAMED('SourceFile'));
    output(Destfile1, NAMED('Destfile1'));
    output(Destfile2, NAMED('Destfile2'));
    #end

    // It would be nice to convert DropZone path (returned by GetDefaultDropZone()) to escaped string
    OUTPUT(JsonDs, , '~file::localhost::var::lib::^H^P^C^C^Systems::mydropzone::' + JsonSourceFileName, OVERWRITE);
    
    // It would be nice to convert DropZone path (returned by GetDefaultDropZone()) to escaped string
    OUTPUT(XmlDs, , '~file::localhost::var::lib::^H^P^C^C^Systems::mydropzone::' + XmlSourceFileName, OVERWRITE);

    FileServices.SprayJson(
                                SOURCEIP := OriginalTextFilesIp,
                                SOURCEPATH :=  JsonSourceFile,
                                DESTINATIONGROUP := ClusterName,
                                DESTINATIONLOGICALNAME :=  DestFile1,
                                TIMEOUT := -1,
                                ESPSERVERIPPORT := ESPportIP,
                                ALLOWOVERWRITE := true
                                );
    output(ds1, NAMED('Ds1'));
    
    
    FileServices.SprayXml(
                        SOURCEIP := OriginalTextFilesIp,
                        SOURCEPATH := XmlSourceFile,
                        SOURCEROWTAG := 'Rowtag',
                        DESTINATIONGROUP := ClusterName,
                        DESTINATIONLOGICALNAME := DestFile2,
                        TIMEOUT := -1,
                        ESPSERVERIPPORT := ESPportIP,
                        ALLOWOVERWRITE := true
                        );
    
    
    output(ds2, NAMED('Ds2'));
    
#if (CLEANUP = 1)
    // Clean-up
    FileServices.DeleteExternalFile('.', JsonSourceFile),
    FileServices.DeleteExternalFile('.', XmlSourceFile),
    FileServices.DeleteLogicalFile(DestFile1),
    FileServices.DeleteLogicalFile(DestFile2),
#end
);
