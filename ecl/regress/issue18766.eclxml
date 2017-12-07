<Archive build="community_6.5.0-trunk0Debug[heads/master-0-g709ab6-dirty]"
         eclVersion="6.5.0"
         legacyImport="0"
         legacyWhen="0">
 <Query attributePath="_local_directory_.teststdlibrary"/>
 <Module key="_local_directory_" name="_local_directory_">
  <Attribute key="teststdlibrary" name="teststdlibrary" sourcePath="/Users/rchapman/HPCC-Platform/testing/regress/ecl/teststdlibrary.ecl">
   /*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the &quot;License&quot;);
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

// In slowest systems where Regression Test Engine executed with --PQ &lt;n&gt; on Thor
//timeout 900

import teststd;

evaluate(teststd.type);
output(&apos;Test std completed&apos;);&#10;
  </Attribute>
 </Module>
 <Module key="teststd.type" name="teststd.Type">
  <Attribute key="testtype" name="TestType" sourcePath="/Users/rchapman/HPCC-Platform/ecllibrary/teststd/Type/TestType.ecl">
   /*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std;

EXPORT TestType := MODULE

  SHARED rec1 := RECORD
    STRING s1;
  END;

  SHARED ds1 := DATASET([{&apos;Hello&apos;}], rec1);

  EXPORT TestSerialize := [
    ASSERT(Std.type.name = &apos;Fred&apos;);
    ASSERT(Std.Type.VRec(rec1).serialize() = x&apos;5052B035DD691DBF6B1300000004040000000D04400004017331000004040000&apos;);  // For some reason CONST fails
    ASSERT(Std.Type.VRec(rec1).serializeJson() =
&apos;&apos;&apos;{
 &quot;ty1&quot;: {
  &quot;fieldType&quot;: 1028,
  &quot;length&quot;: 0
 },
 &quot;fieldType&quot;: 1037,
 &quot;length&quot;: 4,
 &quot;fields&quot;: [
  {
   &quot;name&quot;: &quot;s1&quot;,
   &quot;type&quot;: &quot;ty1&quot;,
   &quot;flags&quot;: 1028
  }
 ]
}&apos;&apos;&apos;);

//    ASSERT(Std.Type.Rec(ds1[1]).serialize() = x&apos;5052B035DD691DBF6B1300000004040000000D04400004017331000004040000&apos;);  // For some reason CONST fails
    ASSERT(Std.Type.Rec(ds1[1]).getStringX(0) = &apos;Hello&apos;);  // Use of ASSERT or compare gives internal error
    ASSERT(Std.Type.Rec(ds1[1]).getStringX(1) = &apos;&apos;);  // Use of ASSERT or compare gives internal error
    ASSERT(Std.Type.Rec(ds1[1]).getStringX(-1) = &apos;&apos;);  // Use of ASSERT or compare gives internal error


    ASSERT(TRUE, CONST)
  ];
END;&#10;
  </Attribute>
 </Module>
 <Module key="std" name="std">
  <Attribute key="type" name="type" sourcePath="/Users/rchapman/HPCC-Platform/ecllibrary/std/Type.ecl">
   /*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.  All rights reserved.
############################################################################## */


EXPORT Type := MODULE
  EXPORT name := &apos;Fred&apos;;
  // Type info for a record
  EXPORT VRec(virtual record outrec) := MODULE

    SHARED externals := SERVICE : fold
      STRING dumpRecordType(virtual record val) : eclrtl,pure,library=&apos;eclrtl&apos;,entrypoint=&apos;dumpRecordType&apos;,fold;
      DATA serializeRecordType(virtual record val) : eclrtl,pure,library=&apos;eclrtl&apos;,entrypoint=&apos;serializeRecordType&apos;,fold;
      STREAMED DATASET(outrec) translate(streamed dataset input, DATA typeinfo) : eclrtl,pure,library=&apos;eclrtl&apos;,entrypoint=&apos;transformRecord&apos;,passParameterMeta(true);
      outrec translateRow(ROW indata, DATA typeinfo) : eclrtl,pure,library=&apos;eclrtl&apos;,entrypoint=&apos;transformRecord&apos;,passParameterMeta(true);
    end;

    /**
     * Returns the binary type metadata structure for a record.
     *
     * @return           A binary representation  of the type information
     */
    EXPORT DATA serialize() := externals.serializeRecordType(outrec);
    EXPORT STRING serializeJson() := externals.dumpRecordType(outrec);
    EXPORT STRING toECL() := &apos;RECORD END;&apos;;

    EXPORT STREAMED DATASET(outrec) translateDataset(streamed dataset input, DATA typeinfo) := externals.translate(input, typeinfo);

    EXPORT outrec translateRecord(ROW input, DATA typeinfo) := externals.translateRow(input, typeinfo);

  END;

  EXPORT Rec(ROW outrec) := MODULE
    SHARED externals := SERVICE : fold
      STRING getStringX(INTEGER4 col, recordof(outrec) row) : eclrtl,pure,library=&apos;eclrtl&apos;,entrypoint=&apos;getFieldVal&apos;,fold,passParameterMeta(true);
    END;

    // Peeking inside the record. Could add more types... Nested datasets are probably not feasible though
    EXPORT STRING   getStringX(UNSIGNED fieldidx) := externals.getStringX(fieldidx, outrec);
    EXPORT INTEGER  getIntX(UNSIGNED fieldidx) := 0;
    EXPORT UNSIGNED getUIntX(UNSIGNED fieldidx) := 0;
    EXPORT REAL8    getRealX(UNSIGNED fieldidx) := 0;
    EXPORT STRING   getTypeX(UNSIGNED fieldidx) := &apos;tbd&apos;;

    EXPORT STRING   getString(STRING fieldname) := &apos;tbd&apos;;
    EXPORT INTEGER  getInt(STRING fieldname) := 0;
    EXPORT UNSIGNED getUInt(STRING fieldname) := 0;
    EXPORT REAL8    getReal(STRING fieldname) := 0;
    EXPORT STRING   getType(STRING fieldidx) := &apos;tbd&apos;;

    EXPORT STRING getFieldName(UNSIGNED idx) := &apos;tbd&apos;;
    EXPORT UNSIGNED getFIeldIdx(STRING name) := 0;
  END;

  // Type info for a record, from serialized data
  EXPORT RecData(DATA outrecPacked) := MODULE

    SHARED externals := SERVICE : fold
      DATA serializeRecordType(virtual record val) : eclrtl,pure,library=&apos;eclrtl&apos;,entrypoint=&apos;serializeRecordType&apos;,fold;
    end;

    /**
     * Returns the binary type metadata structure for a record.
     *
     * @return           A binary representation  of the type information
     */
    EXPORT DATA serialize() := outRecPacked;
    EXPORT STRING serializeJson() := &apos;tbd&apos;;
    EXPORT STRING toECL() := &apos;RECORD END;&apos;;

    // Peeking inside the record. Could add more types... Nested datasets are probably not feasible though
    EXPORT STRING   getStringX(UNSIGNED fieldidx) := &apos;tbd&apos;;
    EXPORT INTEGER  getIntX(UNSIGNED fieldidx) := 0;
    EXPORT UNSIGNED getUIntX(UNSIGNED fieldidx) := 0;
    EXPORT REAL8    getRealX(UNSIGNED fieldidx) := 0;
    EXPORT STRING   getTypeX(UNSIGNED fieldidx) := &apos;tbd&apos;;

    EXPORT STRING   getString(STRING fieldname) := &apos;tbd&apos;;
    EXPORT INTEGER  getInt(STRING fieldname) := 0;
    EXPORT UNSIGNED getUInt(STRING fieldname) := 0;
    EXPORT REAL8    getReal(STRING fieldname) := 0;
    EXPORT STRING   getType(STRING fieldidx) := &apos;tbd&apos;;

    EXPORT STRING getFieldName(UNSIGNED idx) := &apos;tbd&apos;;
    EXPORT UNSIGNED getFIeldIdx(STRING name) := 0;
  END;
END;&#10;
  </Attribute>
 </Module>
 <Option name="spanmultiplecpp" value="0"/>
 <Option name="saveecltempfiles" value="1"/>
 <Option name="savecpptempfiles" value="1"/>
 <Option name="debugquery" value="1"/>
</Archive>
