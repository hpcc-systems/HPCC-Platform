/*##############################################################################

    Copyright (C) 2019 HPCC Systems®.

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

//nohthor
//noroxie

//class=spray

//version xml_header='none',xml_footer='none'
//version xml_header='none',xml_footer='short'
//version xml_header='none',xml_footer='long'
//version xml_header='short',xml_footer='none'
//version xml_header='short',xml_footer='short'
//version xml_header='short',xml_footer='long'
//version xml_header='long',xml_footer='none'
//version xml_header='long',xml_footer='short'
//version xml_header='long',xml_footer='long'

import Std.File AS FileServices;
import $.setup;
import ^ as root;

prefix := setup.Files(false, false).QueryFilePrefix;

dropzonePath := '/var/lib/HPCCSystems/mydropzone/' : STORED('dropzonePath');

espUrl := FileServices.GetEspURL() + '/FileSpray';

unsigned VERBOSE := 0;
unsigned CLEANUP := 1;

string xml_header := #IFDEFINED(root.xml_header, 'short');
#if (xml_header = 'none')
    string header := '';
#elseif (xml_header = 'short')
    string header := '<Header>Head</Header>\n';
#else
    string header := '<Header>Head 123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890</Header>\n';
#end
unsigned expectedHeaderLength := LENGTH(header);

string xml_footer := #IFDEFINED(root.xml_footer, 'none');

#if (xml_footer = 'none')
    string footer := '';
#elseif (xml_footer = 'short')
    string footer := '<Footer>Foot</Footer>';
#else
    string footer := '<Footer>Foot 123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890</Footer>';
#end
unsigned expectedFooterLength := LENGTH(footer);

Layout_Person := RECORD
  STRING3  name;
  UNSIGNED2 age;
  BOOLEAN good;
END;


allPeople := DATASET([ {'foo', 10, 1},
                       {'bar', 12, 0},
                       {'baz', 32, 1}]
            ,Layout_Person);

setupXmlPrepFileName := prefix + 'original_xml';
desprayXmlOutFileName := dropzonePath + prefix + '-desprayed_xml';
sprayXmlTargetFileName := prefix + 'sprayed_xml';
dsSetup := DISTRIBUTE(allPeople);

//  Create a small logical file
setupXmlFile := output(dsSetup, , setupXmlPrepFileName, XML( 'Row', HEADING(header , footer), NOROOT), OVERWRITE);

rec := RECORD
  string result;
  string msg;
end;


// Despray it to default drop zone
rec despray(rec l) := TRANSFORM
  SELF.msg := FileServices.fDespray(
                       LOGICALNAME := setupXmlPrepFileName
                      ,DESTINATIONIP := '.'
                      ,DESTINATIONPATH := desprayXmlOutFileName
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



rec spray(rec l) := TRANSFORM
    SELF.msg := FileServices.fSprayXml(
                        SOURCEIP := '.',
                        SOURCEPATH := desprayXmlOutFileName,
                        SOURCEROWTAG := 'Row',
                        DESTINATIONGROUP := 'mythor',
                        DESTINATIONLOGICALNAME := sprayXmlTargetFileName,
                        TIMEOUT := -1,
                        ESPSERVERIPPORT := espUrl,
                        ALLOWOVERWRITE := true
                        );
    self.result := 'Spray Pass';
end;


dst2 := NOFOLD(DATASET([{'', ''}], rec));
p2 := NOTHOR(PROJECT(NOFOLD(dst2), spray(LEFT)));
c2 := CATCH(NOFOLD(p2), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Spray Fail',
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    sprayOut := output(c2);
#else
    sprayOut := output(c2, {result});
#end

ds := DATASET(sprayXmlTargetFileName, Layout_Person, XML('Row', NOROOT));

string compareDatasets(dataset(Layout_Person) ds1, dataset(Layout_Person) ds2) := FUNCTION
   c := COUNT(JOIN(ds1, ds2, left.name=right.name, FULL ONLY));
   boolean result := (0 = c);
   #if (VERBOSE = 1)
    retVal := 'Compare: ' + if(result, 'Pass', 'Fail') + ', Count = ' + intformat(c, 3, 0);
   #else
    retVal := 'Compare: ' + if(result, 'Pass', 'Fail');
   #end
   RETURN retVal;
END;

string checkFileHeaderFooterLen(string prefix, string fileName, unsigned expectedLen, boolean isHeader) := FUNCTION
    len := (INTEGER) if( isHeader, fileservices.GetLogicalFileAttribute(setupXmlPrepFileName, 'headerLength'), fileservices.GetLogicalFileAttribute(setupXmlPrepFileName, 'footerLength'));
    return ( prefix + ' file ' + if( isHeader, 'header', 'footer')  + ' length: ' +  if (len = expectedLen, 'OK', 'Bad ' + intformat(len, 3, 0) + '/' + intformat(expectedLen, 3, 0)));
end;

SEQUENTIAL(
    setupXmlFile,
    desprayOut,
    sprayOut,
    output(compareDatasets(dsSetup,ds)),
    output(checkFileHeaderFooterLen('Prep', setupXmlPrepFileName, expectedHeaderLength, TRUE));
    output(checkFileHeaderFooterLen('Prep', setupXmlPrepFileName, expectedFooterLength, FALSE));
    output(checkFileHeaderFooterLen('Spray', sprayXmlTargetFileName, expectedHeaderLength, TRUE));
    output(checkFileHeaderFooterLen('Spray', sprayXmlTargetFileName, expectedFooterLength, FALSE));

#if (VERBOSE = 1)
    output(dsSetup, NAMED('dsSetup')),
    output(ds, NAMED('ds')),
    output(JOIN(dsSetup, ds, left.name=right.name, FULL ONLY, LOCAL)),
#end

#if (CLEANUP = 1)
    // Clean-up
    FileServices.DeleteExternalFile('.', desprayXmlOutFileName),
    FileServices.DeleteLogicalFile(setupXmlPrepFileName),
    FileServices.DeleteLogicalFile(sprayXmlTargetFileName)
#end
);
