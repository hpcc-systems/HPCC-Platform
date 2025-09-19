/*##############################################################################
    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.
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

//class=parquet
//nothor
//noroxie
//version clusterName='hthor'
//version clusterName='thor'
//version clusterName='roxie'
#OPTION('pickBestEngine', false);

IMPORT Std;
IMPORT ^ as root;
compressionType := #IFDEFINED(root.compressionType, 'UNCOMPRESSED');

#WORKUNIT('name', 'Submit WU: Test getParquetRecordStructure');

ESP_URL := Std.File.GetEspURL();
USERNAME := '';
USER_PW := '';
CLUSTER_NAME := #IFDEFINED(root.clusterName, 'thor');

//------------------------------------------------------------------------------

soapURL := ESP_URL + '/WsWorkunits/';
auth := IF
    (
        TRIM(USERNAME, ALL) != '',
        'Basic ' + Std.Str.EncodeBase64((DATA)(TRIM(USERNAME, ALL) + ':' + TRIM(USER_PW, LEFT, RIGHT))),
        ''
    );

//------------------------------------------------------------------------------

CreateWorkunit(STRING eclCode) := FUNCTION
    ExceptionsRec := RECORD
        STRING  source      {XPATH('Source')};
        STRING  severity    {XPATH('Severity')};
        INTEGER code        {XPATH('Code')};
        STRING  message     {XPATH('Message')};
        STRING  filename    {XPATH('FileName')};
        INTEGER lineNum     {XPATH('LineNo')};
        INTEGER column      {XPATH('Column')};
    END;

    CreateWorkunitResultRec := RECORD
        STRING  wuid                        {XPATH('Workunit/Wuid')};
        DATASET(ExceptionsRec)  exceptions  {XPATH('Exceptions/Exception')};
    END;

    createWorkunitResult := SOAPCALL
        (
            soapURL,
            'WUUpdate',
            {
                STRING  ecl {XPATH('QueryText')} := eclCode
            },
            CreateWorkunitResultRec,
            XPATH('WUUpdateResponse'),
            HTTPHEADER('Authorization', auth)
        );

    RETURN createWorkunitResult;
END;

//------------------------------------------------------------------------------

SubmitWorkunit(STRING wuid) := FUNCTION
    ExceptionsRec := RECORD
        INTEGER code        {XPATH('Code')};
        STRING  audience    {XPATH('Audience')};
        STRING  source      {XPATH('Source')};
        STRING  message     {XPATH('Message')};
    END;

    submitWorkunitResultRec := RECORD
        STRING  source                      {XPATH('Exceptions/Source')};
        DATASET(ExceptionsRec)  exceptions  {XPATH('Exceptions/Exception')};
    END;

    submitWorkunitResult := SOAPCALL
        (
            soapURL,
            'WUSubmit',
            {
                STRING  wuid                {XPATH('Wuid')} := wuid,
                STRING  clusterName         {XPATH('Cluster')} := CLUSTER_NAME

            },
            submitWorkunitResultRec,
            XPATH('WUSubmitResponse'),
            HTTPHEADER('Authorization', auth)
        );

    WaitWorkunitCompleteResultRec := RECORD
        INTEGER                 stateID     {XPATH('StateID')};
        DATASET(ExceptionsRec)  exceptions  {XPATH('Exceptions/Exception')};
    END;

    waitForWorkunitCompleteResult := SOAPCALL
        (
            soapURL,
            'WUWaitComplete',
            {
                STRING  wuid                {XPATH('Wuid')} := wuid
            },
            WaitWorkunitCompleteResultRec,
            XPATH('WUWaitResponse'),
            HTTPHEADER('Authorization', auth),
            LITERAL
        );

    RETURN IF
        (
            NOT EXISTS(submitWorkunitResult.exceptions),
            waitForWorkunitCompleteResult
        );
END;

//------------------------------------------------------------------------------

GetWorkunitResult(STRING wuid) := FUNCTION
    ExceptionsRec := RECORD
        INTEGER code        {XPATH('Code')};
        STRING  audience    {XPATH('Audience')};
        STRING  source      {XPATH('Source')};
        STRING  message     {XPATH('Message')};
    END;

    submitWorkunitResultRec := RECORD
        STRING  result                          {XPATH('Result')};
        DATASET(ExceptionsRec)  exceptions      {XPATH('Exceptions/Exception')};
    END;

    GetWorkunitResult := SOAPCALL
        (
            soapURL,
            'WUResult',
            {
                STRING  result      {XPATH('Wuid')} := wuid,
                INTEGER seq         {XPATH('Sequence')} := 0
            },
            submitWorkunitResultRec,
            XPATH('WUResultResponse'),
            HTTPHEADER('Authorization', auth)
        );

    RETURN GetWorkunitResult;
END;

//------------------------------------------------------------------------------

IMPORT PARQUET;

// Create a comprehensive test record with all supported data types
childChildRec := RECORD
    UNSIGNED u1 {XPATH('CHILD-CHILD-U1')};
    STRING d1;
    BOOLEAN b1;
END;

childRec := RECORD
    BOOLEAN b1;
    childChildRec cc1;
    SET OF DATA1 sd1;
    INTEGER field1 {XPATH('AAA')}; // Test collision during field name conversion
    INTEGER p_aaa_1;
    INTEGER p_aaa;
END;

parquetSupportedTypesRecord := RECORD
    BOOLEAN     boolField;
    INTEGER1    int1Field {XPATH('INT-1-FIELD')};
    INTEGER2    int2Field;
    INTEGER4    int4Field;
    INTEGER8    int8Field;
    UNSIGNED1   uint1Field {XPATH('UNSIGNED-1-FIELD')};
    UNSIGNED2   uint2Field;
    UNSIGNED4   uint4Field;
    UNSIGNED8   uint8Field;
    REAL4       real4Field;
    REAL8       real8Field;
    DECIMAL10_2 decimalField {XPATH('DECIMAL-FIELD')};
    STRING      stringField;
    VARSTRING   varstringField;
    QSTRING     qstringField;
    UTF8        utf8Field;
    UNICODE     unicodeField;
    DATA        dataField;
    SET OF INTEGER setIntField {XPATH('SET-INT-FIELD')};
    SET OF STRING  setStringField {XPATH('setStringField')};
    childRec refField;
    INTEGER field1 {XPATH('AAA')}; // Test collision during field name conversion
    INTEGER p_aaa_1;
    INTEGER p_aaa;
    INTEGER field4 {XPATH('BBB')};
    INTEGER p_bbb;
    INTEGER p_bbb_1;
END;

// Create test data with a single row
testData := DATASET([{
    TRUE,
    127,
    32767,
    2147483647,
    9223372036854775807,
    255,
    65535,
    4294967295,
    18446744073709551615,
    3.14159,
    2.71828182845904523536,
    123.45,
    'Test String',
    'Test VarString',
    'Test QString',
    U8'Test UTF8 String',
    U'Test Unicode String',
    X'0123456789ABCDEF',
    [1, 2, 3, 4, 5],
    ['apple', 'banana', 'cherry'],
    {TRUE, {255, 'test', FALSE}, [X'A1', D'test'], 42, -42, 314159},
    42,
    -42,
    314159,
    42,
    -42,
    314159
}], parquetSupportedTypesRecord);

// Define the test file path
dropZoneName := Std.File.GetDefaultDropZoneName();
testFileName := WORKUNIT + '-readSchemaTest.parquet';
testFilePath := Std.File.GetDefaultDropZone() + '/regress/parquet/' + testFileName;

// Create the Parquet file, get its record structure, and test it
createTestParquetFile := ParquetIO.Write(testData, testFilePath, TRUE) : INDEPENDENT;
recordStr := ParquetIO.getParquetRecordStructure(testFilePath);
eclStr := 'IMPORT PARQUET;\n\n' + recordStr + 'OUTPUT(PARQUETIO.Read(parquetRecord, \'' + testFilePath + '\'));';
createWorkunitResult := CreateWorkunit(eclStr);
submitWorkunitResult := SubmitWorkunit(createWorkunitResult.wuid);
finalResult := GetWorkunitResult(createWorkunitResult.wuid);

ORDERED(
    // Create the test Parquet file
    createTestParquetFile,

    // Display test record
    OUTPUT(recordStr, NAMED('RecordStructure')),
    // OUTPUT(testFilePath, NAMED('TestFilePath')),

    // Test the workunit creation and execution
    OUTPUT(createWorkunitResult.exceptions, NAMED('createWorkunitResultExceptions')),
    OUTPUT(submitWorkunitResult, NAMED('submitWorkunitResult')),
    OUTPUT(finalResult, NAMED('finalResult')),

    // Clean up the test file
    Std.File.DeleteExternalFile('.', testFilePath, dropZoneName)
);
