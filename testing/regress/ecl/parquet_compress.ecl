/*##############################################################################
    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.
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
//version compressionType='UNCOMPRESSED'
//version compressionType='Snappy'
//version compressionType='GZip'
//version compressionType='Brotli'
//version compressionType='LZ4'
//version compressionType='ZSTD'

IMPORT Std;
IMPORT Parquet;

compressionType := #IFDEFINED(root.compressionType, 'SNAPPY');

// Define record structures
BooleanRec := RECORD
    UNSIGNED testid;
    STRING3 testname;
    BOOLEAN value;
END;

IntegerRec := RECORD
    UNSIGNED testid;
    STRING3 testname;
    INTEGER value;
END;

RealRec := RECORD
    UNSIGNED testid;
    STRING3 testname;
    REAL value;
END;

DecimalRec := RECORD
    UNSIGNED testid;
    STRING3 testname;
    DECIMAL10_2 value;
END;

StringRec := RECORD
    UNSIGNED testid;
    STRING3 testname;
    STRING value;
END;

QStringRec := RECORD
    UNSIGNED testid;
    STRING3 testname;
    STRING value;
END;

UnicodeRec := RECORD
    UNSIGNED testid;
    STRING3 testname;
    UNICODE value;
END;

UTF8Rec := RECORD
    UNSIGNED testid;
    STRING3 testname;
    UTF8 value;
END;

DataRec := RECORD
    UNSIGNED testid;
    STRING3 testname;
    DATA value;
END;

VarstringRec := RECORD
    UNSIGNED testid;
    STRING3 testname;
    VARSTRING value;
END;

VarunicodeRec := RECORD
    UNSIGNED testid;
    STRING3 testname;
    VARUNICODE value;
END;

BooleanData := DATASET([
    {0, 'aaa', TRUE},
    {1, 'aab', FALSE}
], BooleanRec);

IntegerData := DATASET([
    {0, 'min', -9223372036854775808},  // Minimum value for SIGNED8 (64-bit integer)
    {1, 'max', 9223372036854775807}    // Maximum value for SIGNED8 (64-bit integer)
], IntegerRec);

RealData := DATASET([
    {0, 'min', 2.2250738585072014e-308},  // Smallest positive normalized double-precision float
    {1, 'max', 1.7976931348623157e+308}   // Largest finite double-precision float
], RealRec);

DecimalData := DATASET([
    {0, 'max', 9999999999999999999.99},  // Maximum value for DECIMAL32_2
    {1, 'min', -9999999999999999999.99}  // Minimum value for DECIMAL32_2
], DecimalRec);

StringData := DATASET([
    {0, 'empty', ''},
    {1, 'long', 'This is a long string to test the maximum length of a STRING field in HPCC'}
], StringRec);

QStringData := DATASET([
    {0, 'quoted', 'String with "quotes" and spaces  '},
    {1, 'special', 'String with \n newline and \t tab'}
], QStringRec);

UnicodeData := DATASET([
    {0, 'mixed', U'ASCII and Unicode こんにちは'},
    {1, 'emoji', U'Emoji test: 🚀🌟💬😊'}
], UnicodeRec);

UTF8Data := DATASET([
    {0, 'chinese', U'中文测试'},
    {1, 'mixed', U'Mix of scripts: АБВ αβγ こんにちは'}
], UTF8Rec);

DataData := DATASET([
    {0, 'binary', X'0123456789ABCDEF'},
    {1, 'allbits', X'00FF'}  // All bits set in a byte
], DataRec);

VarstringData := DATASET([
    {0, 'short', 'Short'},
    {1, 'long', 'This is a longer varstring to test variable-length behavior'}
], VarstringRec);

VarunicodeData := DATASET([
    {0, 'ascii', U'ASCII only'},
    {1, 'mixed', U'Mixed scripts: Latin, Кириллица, 日本語'}
], VarunicodeRec);

// Write datasets to Parquet files
ParquetIO.Write(BooleanData, '/var/lib/HPCCSystems/mydropzone/Boolean.parquet', TRUE);
ParquetIO.Write(IntegerData, '/var/lib/HPCCSystems/mydropzone/Integer.parquet', TRUE);
ParquetIO.Write(RealData, '/var/lib/HPCCSystems/mydropzone/Real.parquet', TRUE);
ParquetIO.Write(DecimalData, '/var/lib/HPCCSystems/mydropzone/Decimal.parquet', TRUE);
ParquetIO.Write(StringData, '/var/lib/HPCCSystems/mydropzone/String.parquet', TRUE);
ParquetIO.Write(QStringData, '/var/lib/HPCCSystems/mydropzone/QString.parquet', TRUE);
ParquetIO.Write(UnicodeData, '/var/lib/HPCCSystems/mydropzone/Unicode.parquet', TRUE);
ParquetIO.Write(UTF8Data, '/var/lib/HPCCSystems/mydropzone/UTF8.parquet', TRUE);
ParquetIO.Write(DataData, '/var/lib/HPCCSystems/mydropzone/Data.parquet', TRUE);
ParquetIO.Write(VarstringData, '/var/lib/HPCCSystems/mydropzone/Varstring.parquet', TRUE);
ParquetIO.Write(VarunicodeData, '/var/lib/HPCCSystems/mydropzone/Varunicode.parquet', TRUE);

// Read datasets from Parquet files
BooleanDataRead := ParquetIO.Read(BooleanRec, '/var/lib/HPCCSystems/mydropzone/Boolean.parquet');
IntegerDataRead := ParquetIO.Read(IntegerRec, '/var/lib/HPCCSystems/mydropzone/Integer.parquet');
RealDataRead := ParquetIO.Read(RealRec, '/var/lib/HPCCSystems/mydropzone/Real.parquet');
DecimalDataRead := ParquetIO.Read(DecimalRec, '/var/lib/HPCCSystems/mydropzone/Decimal.parquet');
StringDataRead := ParquetIO.Read(StringRec, '/var/lib/HPCCSystems/mydropzone/String.parquet');
QStringDataRead := ParquetIO.Read(QStringRec, '/var/lib/HPCCSystems/mydropzone/QString.parquet');
UnicodeDataRead := ParquetIO.Read(UnicodeRec, '/var/lib/HPCCSystems/mydropzone/Unicode.parquet');
UTF8DataRead := ParquetIO.Read(UTF8Rec, '/var/lib/HPCCSystems/mydropzone/UTF8.parquet');
DataDataRead := ParquetIO.Read(DataRec, '/var/lib/HPCCSystems/mydropzone/Data.parquet');
VarstringDataRead := ParquetIO.Read(VarstringRec, '/var/lib/HPCCSystems/mydropzone/Varstring.parquet');
VarunicodeDataRead := ParquetIO.Read(VarunicodeRec, '/var/lib/HPCCSystems/mydropzone/Varunicode.parquet');

// Output datasets read from Parquet files
OUTPUT(BooleanDataRead, NAMED('BooleanData'));
OUTPUT(IntegerDataRead, NAMED('IntegerData'));
OUTPUT(RealDataRead, NAMED('RealData'));
OUTPUT(DecimalDataRead, NAMED('DecimalData'));
OUTPUT(StringDataRead, NAMED('StringData'));
OUTPUT(QStringDataRead, NAMED('QStringData'));
OUTPUT(UnicodeDataRead, NAMED('UnicodeData'));
OUTPUT(UTF8DataRead, NAMED('UTF8Data'));
OUTPUT(DataDataRead, NAMED('DataData'));
OUTPUT(VarstringDataRead, NAMED('VarstringData'));
OUTPUT(VarunicodeDataRead, NAMED('VarunicodeData'));