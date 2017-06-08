<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
# HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##############################################################################
-->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <xsl:output method="text" version="1.0" encoding="UTF-8" indent="yes"/>
    <xsl:param name="sourceFileName" select="'UNKNOWN'"/>
    <xsl:param name="responseType" select="''"/>
    <xsl:param name="requestType" select="''"/>
    <xsl:param name="platform" select="'roxie'"/>
    <xsl:param name="diffmode" select="'Monitor'"/>
    <xsl:param name="diffaction" select="'Run'"/>
    <xsl:variable name="docname" select="/esxdl/@name"/>
    <xsl:template match="/">
        <xsl:apply-templates select="esxdl"/>
    </xsl:template>
    <xsl:template name="doNotChangeManuallyComment">
        <xsl:text>/*** Generated Code do not hand edit ***/
</xsl:text>
    </xsl:template>

<xsl:template match="esxdl">
  <xsl:call-template name="doNotChangeManuallyComment"/>

<xsl:if test="$diffmode='Monitor'">
IMPORT cassandra;
</xsl:if>

IMPORT std;
IMPORT lib_timelib.TimeLib;
  <xsl:for-each select="ECL/Import">
IMPORT <xsl:value-of select="."/>;<xsl:text>
</xsl:text>
  </xsl:for-each>

  <xsl:for-each select="ECL/Input">
<xsl:value-of select="@type"/><xsl:text> </xsl:text><xsl:value-of select="@name"/><xsl:text> := </xsl:text><xsl:value-of select="@default"/><xsl:text> : STORED('</xsl:text><xsl:value-of select="@name"/><xsl:text>'</xsl:text><xsl:if test="@format">, FORMAT(<xsl:value-of select="@format"/>)</xsl:if><xsl:text>);
</xsl:text>
  </xsl:for-each>

DiffStatus := MODULE

  EXPORT JoinRowType := MODULE
    export integer1 IsInner := 0;
    export integer1 OuterLeft := 1;
    export integer1 OuterRight := 2;
  END;

  // Records in the old and new results are matched by virtual record ID (VID) -- something,
  // which makes "this" record unique. Usually is defined by business logic.
  // If two records have the same VID, they are compared field by field.
  EXPORT State := MODULE
    export integer1 VOID           := 0; // no comparison occured (for example, no alerts were requested)
    export integer1 UNCHANGED      := 1; // no changes
    export integer1 UPDATED        := 2; // there are changes to a scalar field
    export integer1 ADDED          := 4; // record is new (with respect to VID)
    export integer1 DELETED        := 8; // record doesn't exist anymore
    export integer1 PREVIOUS      := 16; // for UPDATED records only: will only have populated fields which are changed
    export integer1 CHILD_UPDATED := 32; // a record has a child dataset which has some records ADDED, DELETED or UPDATED
  END;

  EXPORT string Convert (integer sts) :=
    MAP (sts = State.DELETED   => 'deleted',
         sts = State.ADDED     => 'added',
         sts = State.UPDATED   => 'updated',
         sts = State.UNCHANGED => '',
         '');
END;

request := MODULE<xsl:text>

</xsl:text>
  <xsl:apply-templates select="RequestInfo/EsdlStruct" mode="layouts"/>
  <xsl:apply-templates select="RequestInfo/EsdlRequest" mode="layouts"/>
  <xsl:text>END;
</xsl:text>
layouts := MODULE

  EXPORT DiffString := RECORD
    string7 _diff {XPATH('@diff')} := '';
    string value {XPATH('')} := '';
  END;

  EXPORT DiffStringRow := RECORD  (DiffString)
    integer _diff_ord {xpath('@diff_ord')} := 0;
  END;

  EXPORT DiffMetaRow := RECORD
    string name {XPATH ('@name')};
    string prior {XPATH ('@prior')};
  END;

  EXPORT DiffMetaRec := RECORD
    string7 _child_diff {XPATH('@child_diff')} := '';
    string7 _diff {XPATH('@diff')} := '';
    DATASET (DiffMetaRow) _diffmeta {XPATH ('DiffMeta/Field')} := DATASET ([], DiffMetaRow);
  END;<xsl:text>
</xsl:text>
  <xsl:apply-templates select="EsdlStruct" mode="layouts"/>
  <xsl:apply-templates select="EsdlResponse" mode="layouts"/>
  <xsl:text>END;
</xsl:text>
difference := MODULE<xsl:text>

</xsl:text>
  <xsl:for-each select="Selectors/Selector">
<xsl:text>  EXPORT boolean Monitor</xsl:text><xsl:value-of select="."/> := FALSE : STORED('<xsl:value-of select="$diffmode"/>_<xsl:value-of select="."/>', FORMAT(sequence(<xsl:value-of select="position()+10"/>)));<xsl:text>
</xsl:text>
    </xsl:for-each><xsl:text>
</xsl:text>

EXPORT _df_DiffString(boolean is_active, string path) := MODULE

  EXPORT layouts.DiffStringRow ProcessTxRow(layouts.DiffStringRow L, layouts.DiffStringRow R, integer1 joinRowType) :=TRANSFORM
    boolean is_deleted := joinRowType = DiffStatus.JoinRowType.OuterRight;
    boolean is_added := joinRowType = DiffStatus.JoinRowType.OuterLeft;


    integer _change := MAP (is_deleted  => DiffStatus.State.DELETED,
                      is_added    => DiffStatus.State.ADDED,
                      DiffStatus.State.UNCHANGED);

    SELF._diff := IF(is_active, DiffStatus.Convert (_change), '');
    SELF._diff_ord := IF (is_deleted, R._diff_ord, L._diff_ord);
    SELF := IF (is_deleted, R, L);

  END;

  EXPORT  integer1 CheckOuter(layouts.DiffString L, layouts.DiffString R) := FUNCTION
    boolean IsInner :=  (L.Value = R.Value);
    boolean IsOuterRight :=   (L.Value = '');
    return IF (IsInner, DiffStatus.JoinRowType.IsInner, IF (IsOuterRight, DiffStatus.JoinRowType.OuterRight, DiffStatus.JoinRowType.OuterLeft));
  END;

  EXPORT  AsDataset (dataset(layouts.DiffString) _n, dataset(layouts.DiffString) _o) := FUNCTION

    _new := PROJECT (_n, TRANSFORM (layouts.DiffStringRow, SELF._diff_ord := COUNTER, SELF := LEFT));
    _old := PROJECT (_o, TRANSFORM (layouts.DiffStringRow, SELF._diff_ord := 10000 + COUNTER, SELF := LEFT));
    ActiveJoin := JOIN (_new, _old,
                  LEFT.Value = RIGHT.Value,
                  ProcessTxRow (LEFT, RIGHT,
                  CheckOuter(LEFT, RIGHT)),
                  FULL OUTER,
                  LIMIT (0));
    PassiveJoin := JOIN (_new, _old,
                  LEFT.Value = RIGHT.Value,
                  ProcessTxRow (LEFT, RIGHT,
                  CheckOuter(LEFT, RIGHT)),
                  LEFT OUTER,
                  LIMIT (0));
    RETURN PROJECT(SORT(IF (is_active, ActiveJoin, PassiveJoin), _diff_ord), layouts.DiffString);
  END;

END;

  <xsl:apply-templates select="EsdlStruct" mode="CreateDiffObjectModule"/>
  <xsl:apply-templates select="EsdlRequest" mode="CreateDiffObjectModule"/>
  <xsl:apply-templates select="EsdlResponse" mode="CreateDiffObjectModule"/>
<xsl:text>END;

</xsl:text>
  //Defines
  the_differenceModule := difference._df_<xsl:value-of select="$responseType"/>;
  the_requestLayout := request._lt_<xsl:value-of select="$requestType"/>;
  the_responseLayout := layouts._lt_<xsl:value-of select="$responseType"/>;

<xsl:choose>
  <xsl:when test="$diffmode='Monitor'">
  //Inputs
  string csndServer := '127.0.0.1' : stored('cassandraServer', FORMAT(SEQUENCE(1)));
  string csndUser := '' : stored('cassandraUser', FORMAT(SEQUENCE(2)));
  string csndPassword := '' : stored('cassandraPassword', FORMAT(PASSWORD, SEQUENCE(3)));

  string csndKeySpaceFrom := 'monitors_a' : stored('fromKeyspace', FORMAT(SEQUENCE(4)));
  string csndKeySpaceTo := 'monitors_a' : stored('toKeyspace', FORMAT(SEQUENCE(4)));

  string monAction := 'Create' : STORED('MonAction', FORMAT(SELECT('Create,Run'), SEQUENCE(5)));
  string userId := '' : stored('UserId', FORMAT(SEQUENCE(6)));
  string serviceURL := '' : stored('QueryURL', FORMAT(SEQUENCE(7)));
  string serviceName := '' : stored('QueryName', FORMAT(SEQUENCE(8)));

<xsl:if test="$diffaction='Run'">
  string monitorIdIn := '' : stored('MonitorId', FORMAT(SEQUENCE(9)));
</xsl:if>
  requestIn := DATASET([], the_requestLayout) : STORED ('<xsl:value-of select="$requestType"/>', FEW, FORMAT(FIELDWIDTH(100),FIELDHEIGHT(30), sequence(100)));

  exceptionRec := RECORD
    string10 Source {xpath('Source')};
    integer2 Code {xpath('Code')};
    string100 Message {xpath('Message')};
  END;

  soapoutRec := record <xsl:choose><xsl:when test="$platform='esp'">(the_responseLayout)
    </xsl:when>
    <xsl:otherwise>
    dataset (the_responseLayout) ds {xpath('Dataset/Row')};
    </xsl:otherwise>
  </xsl:choose>
    exceptionRec Exception {xpath('Exception')};
  end;

MonSoapcall(DATASET(the_requestLayout) req) := FUNCTION

  <xsl:choose>
    <xsl:when test="$platform='esp'">
  //ESP uses the request layout as is
  ds_request := req;

    </xsl:when>
    <xsl:otherwise>
  // When calling roxie the actual request parameters are placed inside a dataset that is named the same as the request
  // so it looks like:
  //   <MyRequest><MyRequest><Row><User>...</User><Options>..</Options><SearchBy>...</SearchBy></Row></MyRequest></MyRequest>

  in_rec := record
    DATASET (the_requestLayout) <xsl:value-of select="$requestType"/> {xpath('<xsl:value-of select="$requestType"/>/Row'), maxcount(1)};
  end;
  in_rec Format () := transform
    Self.<xsl:value-of select="$requestType"/> := req;
  end;

  ds_request := DATASET ([Format()]);
    </xsl:otherwise>
  </xsl:choose>


  // execute soapcall
  ar_results := SOAPCALL (ds_request,
                          serviceURL,
                          serviceName,
                          {ds_request},
                          DATASET (soapoutRec),
                          TIMEOUT(6), RETRY(1), LITERAL, XPATH('*<xsl:if test="$platform!='esp'">/Results/Result</xsl:if>'));
  RETURN ar_results;
END;

  monitorStoreRec := RECORD
    string MonitorId,
    string result
  END;

// Initialize the Cassandra table, passing in the ECL dataset to provide the rows
// When not using batch mode, maxFutures controls how many simultaenous writes to Cassandra are allowed before
// we start to throttle, and maxRetries controls how many times inserts that fail because Cassandra is too busy
// will be retried.

monitorStoreRec getStoredMonitor(string id) := EMBED(cassandra : server(csndServer), user(csndUser), password(csndPassword), keyspace(csndKeySpaceFrom))
  SELECT monitorId, result from monitor WHERE monitorId=? LIMIT 1;
ENDEMBED;

updateMonitor(dataset(monitorStoreRec) values) := EMBED(cassandra : server(csndServer), user(csndUser), password(csndPassword), keyspace(csndKeySpaceTo), maxFutures(100), maxRetries(10))
  INSERT INTO monitor (monitorId, result) values (?,?);
ENDEMBED;

  MonitorResultRec := RECORD
    string id;
    string responseXML;
    dataset(the_responseLayout) report;
//    dataset(soapoutRec) soap;
//    dataset(the_responseLayout) prior;
  END;

CreateMonitor (string userid, dataset(the_requestLayout) req) := MODULE
  requestXML := TOXML(req[1]);
  DATA16 monitorHash := HASHMD5(userId, requestXml, TimeLib.CurrentTimestamp(false));
  SHARED string monitorId := STD.Str.ToHexPairs(monitorHash);
  SHARED soapOut := MonSoapCall(req)[1];
  <xsl:choose>
    <xsl:when test="$platform='esp'">
  SHARED responseRow := soapOut;
    </xsl:when>
    <xsl:otherwise>
  SHARED responseRow := soapOut.ds[1];
    </xsl:otherwise>
  </xsl:choose>

  SHARED responseXML := '&lt;Row&gt;' + TOXML(responseRow) + '&lt;/Row&gt;';

  SHARED MonitorResultRec BuildMonitor() :=TRANSFORM
    SELF.id := IF (soapOut.Exception.Code=0, monitorId, ERROR(soapOut.Exception.Code, soapOut.Exception.Message));
    SELF.responseXML := (string) responseXML;
    SELF.report := responseRow;
//    SELF.soap := soapOut;
//    SELF.prior := DATASET([], the_responseLayout);
  END;
  EXPORT Result () := FUNCTION
    RETURN ROW(BuildMonitor());
  END;
END;

RunMonitor (string id, dataset(the_requestLayout) req) := MODULE
  SHARED monitorId := id;
  SHARED monitorStore := getStoredMonitor(id);
  SHARED soapOut := MonSoapCall(req)[1];
  <xsl:choose>
    <xsl:when test="$platform='esp'">
  SHARED responseRow := soapOut;
    </xsl:when>
    <xsl:otherwise>
  SHARED responseRow := soapOut.ds[1];
    </xsl:otherwise>
  </xsl:choose>
  SHARED responseXML := '&lt;Row&gt;' + TOXML(responseRow) + '&lt;/Row&gt;';

  SHARED oldResponse := FROMXML (the_responseLayout, monitorStore.result);

  SHARED diff_result := the_differenceModule(false, '').AsRecord(responseRow, oldResponse);

  EXPORT MonitorResultRec BuildMonitor() :=TRANSFORM
    SELF.id := IF (soapOut.Exception.Code=0, monitorId, ERROR(soapOut.Exception.Code, soapOut.Exception.Message));
    SELF.responseXML := (string) responseXML;
    SELF.report := diff_result;
//    SELF.soap := soapOut;
//    SELF.prior := oldResponse;
  END;
  EXPORT Result () := FUNCTION
    RETURN ROW(BuildMonitor());
  END;
END;

<xsl:choose>
  <xsl:when test="$diffaction='Create'">
  executedAction := CreateMonitor(userId, requestIn).Result();
  </xsl:when>
  <xsl:otherwise>
  executedAction := RunMonitor(monitorIdIn, requestIn).Result();
  </xsl:otherwise>
</xsl:choose>

  output(executedAction.id, NAMED('MonitorId'));
  output(executedAction.report, NAMED('Result'));
//  output(executedAction.soap, NAMED('SOAP'));
//  output(executedAction.prior, NAMED('PRIOR'));
  updateMonitor(DATASET([{executedAction.id, executedAction.responseXML}], monitorStoreRec));


  <xsl:if test="Template//*[@diff_monitor]">
  CategoryPathsRec := RECORD
    string categories {xpath('@categories')};
    string path{xpath('@path')};
  END;

    <xsl:text>OUTPUT(DATASET([</xsl:text>
    <xsl:for-each select="Template//*[@diff_monitor]">
        <xsl:if test="position()!=1"><xsl:text>,</xsl:text></xsl:if>
        <xsl:text>{'</xsl:text><xsl:value-of select="@diff_monitor"/><xsl:text>','</xsl:text>
        <xsl:for-each select="ancestor-or-self::*">
          <xsl:if test="position()>2">
            <xsl:text>/</xsl:text><xsl:value-of select="name()"/>
          </xsl:if>
        </xsl:for-each>
      <xsl:text>'}</xsl:text>
      </xsl:for-each>
    <xsl:text>], CategoryPathsRec), NAMED('Categories'));</xsl:text>
  </xsl:if>

  </xsl:when>
  <xsl:when test="$diffmode='Compare'">
  STRING originalXML := '' : STORED ('original', FORMAT(FIELDWIDTH(100), FIELDHEIGHT(30), sequence(1001)));
  STRING changedXML := '' : STORED ('changed', FORMAT(FIELDWIDTH(100), FIELDHEIGHT(30), sequence(1002)));

  originalRow := FROMXML (the_responseLayout, originalXML);
  changedRow := FROMXML (the_responseLayout, changedXML);

  OUTPUT(the_differenceModule(false, '').AsRecord(changedRow, originalRow), NAMED('Difference'));

  </xsl:when>
</xsl:choose>
  <xsl:if test="Selectors/Selector">
  SelectorRec := RECORD
    string monitor {xpath('@monitor')};
    boolean active {xpath('@active')};
  END;

    <xsl:text>OUTPUT(DATASET([</xsl:text>
    <xsl:for-each select="Selectors/Selector">
      <xsl:if test="position()!=1"><xsl:text>,</xsl:text></xsl:if>
      <xsl:text>{'</xsl:text><xsl:value-of select="."/>', difference.Monitor<xsl:value-of select="."/><xsl:text>}</xsl:text>
    </xsl:for-each>], SelectorRec), NAMED('Selected'));
  </xsl:if>

  <xsl:call-template name="doNotChangeManuallyComment"/>
</xsl:template>

<!-- Difference -->

<xsl:template match="EsdlStruct|EsdlRequest|EsdlResponse" mode="DiffScalarsCompareChildren">
  <xsl:if test="@base_type">
    <xsl:variable name="base_type" select="@base_type"/>
    <xsl:apply-templates select="/esxdl/EsdlStruct[@name=$base_type]" mode="DiffScalarsCompareChildren"/>
  </xsl:if>
  <xsl:apply-templates select="EsdlElement[@type]|EsdlEnumRef" mode="CheckCompare"/>
</xsl:template>

<xsl:template match="EsdlStruct|EsdlRequest|EsdlResponse" mode="ProcessTxChildren">
  <xsl:if test="@base_type">
    <xsl:variable name="base_type" select="@base_type"/>
    <xsl:apply-templates select="/esxdl/EsdlStruct[@name=$base_type]" mode="ProcessTxChildren"/>
  </xsl:if>
  <xsl:apply-templates select="EsdlElement[@complex_type]|EsdlArray" mode="CheckCompare"/>
</xsl:template>

<xsl:template match="EsdlStruct|EsdlRequest|EsdlResponse" mode="GatherUpdateFlagsChildren">
  <xsl:if test="@base_type">
    <xsl:variable name="base_type" select="@base_type"/>
    <xsl:apply-templates select="/esxdl/EsdlStruct[@name=$base_type]" mode="GatherUpdateFlagsChildren"/>
  </xsl:if>
  <xsl:apply-templates select="EsdlElement[@type]" mode="AppendUpdateFlag"/>
</xsl:template>

<xsl:template match="EsdlElement[@type and (not(@_nomon) or @_mon='1')]" mode="AppendUpdateFlag">
      OR updated_<xsl:call-template name="output_ecl_name"/>
</xsl:template>


<xsl:template match="EsdlStruct|EsdlRequest|EsdlResponse" mode="BuildMetaDataChildren">
    <xsl:variable name="base_content">
      <xsl:if test="@base_type">
        <xsl:variable name="base_type" select="@base_type"/>
        <xsl:apply-templates select="/esxdl/EsdlStruct[@name=$base_type]" mode="BuildMetaDataChildren"/>
      </xsl:if>
    </xsl:variable>
  <xsl:variable name="local_content">
    <xsl:apply-templates select="EsdlElement[@type]" mode="AppendMetaData"/>
  </xsl:variable>
  <xsl:if test="string($base_content)">
    <xsl:value-of select="$base_content"/>
    <xsl:if test="string($local_content)">+</xsl:if>
  </xsl:if>
  <xsl:value-of select="$local_content"/>
</xsl:template>

<xsl:template match="EsdlElement[@type and (not(@_nomon) or @_mon='1')]" mode="AppendMetaData">
  <xsl:if test="position()!=1"><xsl:text>
         + </xsl:text></xsl:if> IF (updated_<xsl:call-template name="output_ecl_name"/>, DATASET ([{'<xsl:call-template name="output_ecl_name"/><xsl:text>', </xsl:text>
  <xsl:choose>
    <xsl:when test="@type='bool'">IF(R.<xsl:call-template name="output_ecl_name"/> = true, 'true', 'false')</xsl:when>
    <xsl:otherwise>R.<xsl:call-template name="output_ecl_name"/></xsl:otherwise>
  </xsl:choose>
<xsl:text>}], layouts.DiffMetaRow))</xsl:text>
</xsl:template>

<xsl:template match="*[@mon_child]" mode="CreateDiffObjectModule">
  <xsl:variable name="struct_name"><xsl:call-template name="output_ecl_name"/></xsl:variable>
  <xsl:text>EXPORT _df_</xsl:text><xsl:value-of select="$struct_name"/>(boolean is_active, string path) := MODULE<xsl:text>
  </xsl:text>EXPORT layouts._lt_<xsl:value-of select="$struct_name"/> ProcessTx(layouts._lt_<xsl:value-of select="$struct_name"/> L, layouts._lt_<xsl:value-of select="$struct_name"/> R) :=TRANSFORM

  <xsl:apply-templates select="." mode="UnmonitoredDiffModuleChildren"/>
<xsl:text>END;
</xsl:text>
    EXPORT AsRecord (layouts._lt_<xsl:call-template name="output_ecl_name"/> _new, layouts._lt_<xsl:call-template name="output_ecl_name"/> _old) := FUNCTION
      RETURN ROW (ProcessTx(_new, _old));
    END;

<xsl:text>
END;
</xsl:text>
</xsl:template>


<xsl:template match="*[@diff_monitor]" mode="CreateDiffObjectModule">
<xsl:if test="not(@_base) or @_used">
  <xsl:variable name="struct_name"><xsl:call-template name="output_ecl_name"/></xsl:variable>
  <xsl:text>EXPORT _df_</xsl:text><xsl:value-of select="$struct_name"/>(boolean is_active, string path) := MODULE
<xsl:if test="EsdlElement[@type]">
<xsl:text>
  </xsl:text>EXPORT DiffScalars (layouts._lt_<xsl:value-of select="$struct_name"/> L, layouts._lt_<xsl:value-of select="$struct_name"/> R, boolean is_deleted, boolean is_added) := MODULE<xsl:text>
</xsl:text>
  <xsl:apply-templates select="." mode="DiffScalarsCompareChildren"/><xsl:text>
    shared is_updated := false</xsl:text>
  <xsl:apply-templates select="." mode="GatherUpdateFlagsChildren"/>;

    shared integer _change := MAP (is_deleted  => DiffStatus.State.DELETED,
                      is_added    => DiffStatus.State.ADDED,
                      is_updated  => DiffStatus.State.UPDATED,
                      DiffStatus.State.UNCHANGED);

    EXPORT _diff := DiffStatus.Convert (_change);
    // Get update information for all scalars
    _meta :=  <xsl:apply-templates select="." mode="BuildMetaDataChildren"/>;

    EXPORT _diffmeta := IF (~is_deleted AND ~is_added AND is_updated, _meta);
  END;
  </xsl:if>
<xsl:text>
  </xsl:text>EXPORT layouts._lt_<xsl:value-of select="$struct_name"/> ProcessTx(layouts._lt_<xsl:value-of select="$struct_name"/> L, layouts._lt_<xsl:value-of select="$struct_name"/> R, boolean is_deleted, boolean is_added) :=TRANSFORM
<xsl:if test="EsdlElement[@type]">
      m := DiffScalars(L, R, is_deleted, is_added);

      SELF._diff := IF(is_active, m._diff, '');
      SELF._diffmeta := IF(is_active, m._diffmeta);
</xsl:if>
  <xsl:apply-templates select="." mode="ProcessTxChildren"/>

      SELF := IF (is_deleted, R, L);
<xsl:text>
    END;

</xsl:text>

<xsl:if test="@_usedInArray">
<xsl:text>
  </xsl:text>EXPORT layouts._lt_row_<xsl:value-of select="$struct_name"/> ProcessTxRow(layouts._lt_row_<xsl:value-of select="$struct_name"/> L, layouts._lt_row_<xsl:value-of select="$struct_name"/> R, integer1 joinRowType) :=TRANSFORM
    boolean is_deleted := joinRowType = DiffStatus.JoinRowType.OuterRight;
    boolean is_added := joinRowType = DiffStatus.JoinRowType.OuterLeft;
<xsl:if test="EsdlElement[@type]">
    m := DiffScalars(L, R, is_deleted, is_added);

    SELF._diff := IF(is_active, m._diff, '');
    SELF._diffmeta := IF(is_active, m._diffmeta);
</xsl:if>
   <xsl:apply-templates select="." mode="ProcessTxChildren"/>
    SELF._diff_ord := IF (is_deleted, R._diff_ord, L._diff_ord);
    SELF := IF (is_deleted, R, L);
<xsl:text>
  END;

</xsl:text>
</xsl:if>
  EXPORT AsRecord (layouts._lt_<xsl:call-template name="output_ecl_name"/> _new, layouts._lt_<xsl:call-template name="output_ecl_name"/> _old) := FUNCTION
    RETURN ROW (ProcessTx(_new, _old, false, false));
  END;
  <xsl:for-each select="DiffMatchs/diff_match">
    <xsl:variable name="vid_name" select="translate(@name, ' .', '__')"/>
  EXPORT  integer1 CheckOuter_<xsl:value-of select="$vid_name"/>(layouts._lt_<xsl:value-of select="$struct_name"/> L, layouts._lt_<xsl:value-of select="$struct_name"/> R) := FUNCTION
    boolean IsInner := <xsl:text> (</xsl:text>
        <xsl:for-each select="part">
          <xsl:if test="position()!=1"> AND </xsl:if>L.<xsl:value-of select="@name"/><xsl:text> = </xsl:text>R.<xsl:value-of select="@name"/>
        </xsl:for-each>);

    boolean IsOuterRight :=  <xsl:text> (</xsl:text>
      <xsl:for-each select="part">
        <xsl:if test="position()!=1"> AND </xsl:if>L.<xsl:value-of select="@name"/><xsl:text> = </xsl:text>
        <xsl:choose>
          <xsl:when test="@ftype='number'"><xsl:text>0</xsl:text></xsl:when>
          <xsl:when test="@ftype='bool'"><xsl:text>false</xsl:text></xsl:when>
          <xsl:when test="@ftype='float'"><xsl:text>0.0</xsl:text></xsl:when>
          <xsl:otherwise><xsl:text>''</xsl:text></xsl:otherwise>
        </xsl:choose>
      </xsl:for-each>);
    return IF (IsInner, DiffStatus.JoinRowType.IsInner, IF (IsOuterRight, DiffStatus.JoinRowType.OuterRight, DiffStatus.JoinRowType.OuterLeft));
  END;
  EXPORT  AsDataset_<xsl:value-of select="$vid_name"/> (dataset(layouts._lt_<xsl:value-of select="$struct_name"/>) _n, dataset(layouts._lt_<xsl:value-of select="$struct_name"/>) _o) := FUNCTION

    _new := PROJECT (_n, TRANSFORM (layouts._lt_row_<xsl:value-of select="$struct_name"/>, SELF._diff_ord := COUNTER, SELF := LEFT));
    _old := PROJECT (_o, TRANSFORM (layouts._lt_row_<xsl:value-of select="$struct_name"/>, SELF._diff_ord := 10000 + COUNTER, SELF := LEFT));
    ActiveJoin := JOIN (_new, _old,<xsl:text>
                  </xsl:text>
    <xsl:for-each select="part">
      <xsl:if test="position()!=1"> AND </xsl:if>LEFT.<xsl:value-of select="@name"/> = RIGHT.<xsl:value-of select="@name"/>
    </xsl:for-each>,
                  ProcessTxRow (LEFT, RIGHT,
                  CheckOuter_<xsl:value-of select="$vid_name"/>(LEFT, RIGHT)),
                  FULL OUTER,
                  LIMIT (0));
    PassiveJoin := JOIN (_new, _old,<xsl:text>
                  </xsl:text>
    <xsl:for-each select="part">
      <xsl:if test="position()!=1"> AND </xsl:if>LEFT.<xsl:value-of select="@name"/> = RIGHT.<xsl:value-of select="@name"/>
    </xsl:for-each>,
                  ProcessTxRow (LEFT, RIGHT,
                  CheckOuter_<xsl:value-of select="$vid_name"/>(LEFT, RIGHT)),
                  LEFT OUTER,
                  LIMIT (0));
    RETURN PROJECT(SORT(IF (is_active, ActiveJoin, PassiveJoin), _diff_ord), layouts._lt_<xsl:value-of select="$struct_name"/>);
  END;
  </xsl:for-each>
<xsl:text>
END;

</xsl:text>
</xsl:if>
</xsl:template>


<xsl:template match="*" mode="CreateDiffObjectModule"></xsl:template>

<xsl:template match="EsdlStruct|EsdlRequest|EsdlResponse" mode="UnmonitoredDiffModuleChildren">
  <xsl:if test="@base_type">
    <xsl:variable name="base_type" select="@base_type"/>
    <xsl:apply-templates select="/esxdl/EsdlStruct[@name=$base_type]" mode="UnmonitoredDiffModuleChildren"/>
  </xsl:if>
  <xsl:apply-templates select="*" mode="UnmonitoredDiffModuleChildren"/>
</xsl:template>


<xsl:template match="EsdlElement[@complex_type]" mode="UnmonitoredDiffModuleChildren">
<xsl:choose>
  <xsl:when test="@mon_child | @_mon">
    <xsl:variable name="type" select="@complex_type"/>
    <xsl:variable name="field"><xsl:call-template name="output_ecl_name"/></xsl:variable>
      SELF.<xsl:value-of select="$field"/> := _df_<xsl:value-of select="$type"/><xsl:text>(</xsl:text>
<xsl:call-template name="output_active_check">
  <xsl:with-param name="pathvar">path + '/<xsl:call-template name="output_ecl_name"/>'</xsl:with-param>
</xsl:call-template>
<xsl:text>, path + '/</xsl:text><xsl:value-of select="$field"/>').AsRecord(L.<xsl:value-of select="$field"/>, R.<xsl:value-of select="$field"/>);
  </xsl:when>
  <xsl:otherwise>
      SELF.<xsl:call-template name="output_ecl_name"/> := L.<xsl:call-template name="output_ecl_name"/>;
  </xsl:otherwise>
</xsl:choose>
</xsl:template>

<xsl:template match="EsdlElement[@type]" mode="UnmonitoredDiffModuleChildren">
      SELF.<xsl:call-template name="output_ecl_name"/> := L.<xsl:call-template name="output_ecl_name"/>;
</xsl:template>

<xsl:template match="EsdlArray[DiffMatchs/diff_match and (@mon_child='1' or @_mon='1')]" mode="UnmonitoredDiffModuleChildren">
    <xsl:variable name="vid_name" select="translate(DiffMatchs/diff_match/@name, ' .', '__')"/>
    <xsl:variable name="type" select="@type"/>
    <xsl:variable name="field"><xsl:call-template name="output_ecl_name"/></xsl:variable>
      SELF.<xsl:call-template name="output_ecl_name"/>  := _df_<xsl:value-of select="$type"/><xsl:text>(</xsl:text>
<xsl:call-template name="output_active_check">
  <xsl:with-param name="pathvar">path + '/<xsl:value-of select="$field"/>'</xsl:with-param>
</xsl:call-template>
<xsl:text>, path + '/</xsl:text><xsl:call-template name="output_ecl_name"/>/<xsl:call-template name="output_item_tag"/>').AsDataset_<xsl:value-of select="$vid_name"/>(L.<xsl:call-template name="output_ecl_name"/>, R.<xsl:call-template name="output_ecl_name"/>);
</xsl:template>

<xsl:template match="EsdlArray[@type='string']" mode="UnmonitoredDiffModuleChildren">
    <xsl:variable name="field"><xsl:call-template name="output_ecl_name"/></xsl:variable>
      SELF.<xsl:value-of select="$field"/>  := _df_DiffString<xsl:text>(</xsl:text>
<xsl:call-template name="output_active_check">
  <xsl:with-param name="pathvar">path + '/<xsl:value-of select="$field"/>'</xsl:with-param>
</xsl:call-template>
<xsl:text>, path + '/</xsl:text><xsl:value-of select="$field"/>/<xsl:call-template name="output_item_tag"/>').AsDataset(L.<xsl:value-of select="$field"/>, R.<xsl:value-of select="$field"/>);
</xsl:template>

<xsl:template match="EsdlArray" mode="UnmonitoredDiffModuleChildren">
        SELF.<xsl:call-template name="output_ecl_name"/> := L.<xsl:call-template name="output_ecl_name"/>;
</xsl:template>

<xsl:template match="DiffMatchs|_diff_selectors" mode="UnmonitoredDiffModuleChildren">
</xsl:template>



<xsl:template match="EsdlElement[@complex_type]" mode="CheckCompare">
    <xsl:variable name="type" select="@complex_type"/>
    <xsl:variable name="field"><xsl:call-template name="output_ecl_name"/></xsl:variable>
    <xsl:choose>
      <xsl:when test="@_nomon='1' and (not(@_mon) or @_mon='0')">
<xsl:text>      SELF.</xsl:text><xsl:call-template name="output_ecl_name"/>  := L.<xsl:call-template name="output_ecl_name"/><xsl:text>;
</xsl:text>
      </xsl:when>
      <xsl:otherwise>
      path_<xsl:value-of select="$field"/> := path + '/<xsl:value-of select="$field"/>';
    <xsl:choose>
      <xsl:when test="@_nomon='1'">
      updated_<xsl:value-of select="$field"/> := IF (path_<xsl:value-of select="$field"/> IN optional_fields, _df_<xsl:value-of select="$type"/><xsl:text>(</xsl:text>
<xsl:call-template name="output_active_check">
  <xsl:with-param name="pathvar">path_<xsl:value-of select="$field"/></xsl:with-param>
</xsl:call-template>
<xsl:text>, path_</xsl:text><xsl:value-of select="$field"/>).AsRecord(L.<xsl:value-of select="$field"/>, R.<xsl:value-of select="$field"/>), L.R.<xsl:value-of select="$field"/>);
        </xsl:when>
        <xsl:otherwise>
      updated_<xsl:value-of select="$field"/> := _df_<xsl:value-of select="$type"/><xsl:text>(</xsl:text>
<xsl:call-template name="output_active_check">
  <xsl:with-param name="pathvar">path_<xsl:value-of select="$field"/></xsl:with-param>
</xsl:call-template>
<xsl:text>, path_</xsl:text><xsl:value-of select="$field"/>).AsRecord(L.<xsl:value-of select="$field"/>, R.<xsl:value-of select="$field"/>);
        </xsl:otherwise>
        </xsl:choose>
      checked_<xsl:value-of select="$field"/> := MAP (is_deleted => R.<xsl:value-of select="$field"/>,
                              is_added => L.<xsl:value-of select="$field"/>,
                              updated_<xsl:value-of select="$field"/>);
      SELF.<xsl:value-of select="$field"/> := checked_<xsl:value-of select="$field"/>;<xsl:text>
</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
</xsl:template>

<xsl:template match="EsdlElement[@type and (not(@_nomon) or @_mon='1')]" mode="CheckCompare">
    <xsl:variable name="field"><xsl:call-template name="output_ecl_name"/></xsl:variable>
    <xsl:if test="diff_selectors/entry">
      <xsl:text>    </xsl:text><xsl:value-of select="$field"/><xsl:text>_active := </xsl:text>
 <xsl:call-template name="output_active_check">
  <xsl:with-param name="pathvar">path + '/<xsl:value-of select="$field"/>'</xsl:with-param>
</xsl:call-template>
<xsl:text>;
</xsl:text>
</xsl:if>
    <xsl:if test="diff_compare">
      <xsl:text>    </xsl:text><xsl:value-of select="$field"/>_path := path + '/<xsl:value-of select="$field"/>';<xsl:text>
</xsl:text>
</xsl:if>
    <xsl:text>    </xsl:text>shared boolean updated_<xsl:value-of select="$field"/><xsl:text> := </xsl:text><xsl:if test="diff_selectors/entry"><xsl:value-of select="$field"/><xsl:text>_active AND </xsl:text></xsl:if>

    <xsl:if test="diff_compare">
      <xsl:text>CASE(</xsl:text><xsl:value-of select="$field"/><xsl:text>_path,
</xsl:text>
  <xsl:for-each select="diff_compare">
    <xsl:if test="position()!=1"><xsl:text>,
</xsl:text>
    </xsl:if>
<xsl:text>      '</xsl:text><xsl:value-of select="@xpath"/>' => (<xsl:value-of select="@compare"/><xsl:text>)</xsl:text>
  </xsl:for-each>
  <xsl:text>,
      </xsl:text>
</xsl:if>
  <xsl:choose>
    <xsl:when test="@diff_comp">(<xsl:value-of select="@diff_comp"/>)</xsl:when>
    <xsl:otherwise>(L.<xsl:value-of select="$field"/> != R.<xsl:value-of select="$field"/>)</xsl:otherwise>
  </xsl:choose>
  <xsl:if test="diff_compare"><xsl:text>
    )</xsl:text>
  </xsl:if>
  <xsl:text>;
</xsl:text>
</xsl:template>

<xsl:template match="EsdlArray" mode="CheckCompare">
  <xsl:variable name="field"><xsl:call-template name="output_ecl_name"/></xsl:variable>
  <xsl:choose>
  <xsl:when test="@type='string'">
      updated_<xsl:value-of select="$field"/> := _df_DiffString<xsl:text>(</xsl:text>
    <xsl:call-template name="output_active_check">
      <xsl:with-param name="pathvar">path + '/<xsl:value-of select="$field"/>'</xsl:with-param>
    </xsl:call-template>
    <xsl:text>, path + '/</xsl:text><xsl:value-of select="$field"/>/<xsl:call-template name="output_item_tag"/>').AsDataset(L.<xsl:value-of select="$field"/>, R.<xsl:value-of select="$field"/>);
      checked_<xsl:value-of select="$field"/> := MAP (is_deleted => R.<xsl:value-of select="$field"/>,
                              is_added => L.<xsl:value-of select="$field"/>,
                              updated_<xsl:value-of select="$field"/>);
      SELF.<xsl:value-of select="$field"/> := checked_<xsl:value-of select="$field"/><xsl:text>;
    </xsl:text>
  </xsl:when>
  <xsl:when test="DiffMatchs/diff_match and (not(@_nomon) or @_mon='1')">
    <xsl:variable name="vid_name" select="translate(DiffMatchs/diff_match/@name, ' .', '__')"/>
    <xsl:variable name="type" select="@type"/>
      updated_<xsl:call-template name="output_ecl_name"/> := _df_<xsl:value-of select="$type"/><xsl:text>(</xsl:text>
<xsl:call-template name="output_active_check">
  <xsl:with-param name="pathvar">path + '/<xsl:value-of select="$field"/>'</xsl:with-param>
</xsl:call-template>
<xsl:text>, path + '/</xsl:text><xsl:call-template name="output_ecl_name"/>/<xsl:call-template name="output_item_tag"/>').AsDataset_<xsl:value-of select="$vid_name"/>(L.<xsl:call-template name="output_ecl_name"/>, R.<xsl:call-template name="output_ecl_name"/>);
      checked_<xsl:value-of select="$field"/> := MAP (is_deleted => R.<xsl:value-of select="$field"/>,
                              is_added => L.<xsl:value-of select="$field"/>,
                              updated_<xsl:value-of select="$field"/>);
      SELF.<xsl:call-template name="output_ecl_name"/>  := checked_<xsl:call-template name="output_ecl_name"/><xsl:text>;
</xsl:text>
  </xsl:when>
  <xsl:otherwise>
<xsl:text>      SELF.</xsl:text><xsl:call-template name="output_ecl_name"/>  := L.<xsl:call-template name="output_ecl_name"/><xsl:text>;
</xsl:text>
  </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:template match="EsdlEnum" mode="CheckCompare">
  <xsl:variable name="entype" select="@enum_type"/>
</xsl:template>

<xsl:template match="DiffMatchs|_diff_selectors" mode="CheckCompare">
</xsl:template>

<!-- Layouts -->

<xsl:template match="EsdlElement[@complex_type]" mode="layouts">
    <xsl:text>    _lt_</xsl:text><xsl:call-template name="output_ecl_complex_type"/><xsl:text> </xsl:text><xsl:call-template name="output_ecl_name"/>
    <xsl:text> {</xsl:text><xsl:call-template name="output_xpath"/><xsl:text>};</xsl:text><xsl:call-template name="output_comments"/><xsl:text>
</xsl:text>
</xsl:template>

<xsl:template match="EsdlElement[@type]" mode="layouts">
    <xsl:text>    </xsl:text><xsl:call-template name="output_basic_type"/><xsl:text> </xsl:text><xsl:call-template name="output_ecl_name"/>
    <xsl:text> {</xsl:text><xsl:call-template name="output_xpath"/>
    <xsl:if test="@ecl_max_len"><xsl:text>, maxlength(</xsl:text><xsl:value-of select="@ecl_max_len"/><xsl:text>)</xsl:text></xsl:if>
    <xsl:text>};</xsl:text><xsl:call-template name="output_comments"/>
<xsl:text>
</xsl:text>
</xsl:template>

<xsl:template match="EsdlEnum" mode="layouts">
  <xsl:variable name="entype" select="@enum_type"/>
  <xsl:text>    </xsl:text><xsl:call-template name="output_enum_type"/><xsl:text> </xsl:text><xsl:call-template name="output_ecl_name"/>
  <xsl:text> {</xsl:text><xsl:call-template name="output_xpath"/><xsl:text>}; </xsl:text><xsl:call-template name="output_comments"/><xsl:text>
</xsl:text>
</xsl:template>

<xsl:template match="EsdlArray[@type='string']" mode="layouts">
    <xsl:choose>
      <xsl:when test="name(../..)='RequestInfo'">
        <xsl:text>    SET OF STRING </xsl:text><xsl:call-template name="output_ecl_name"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>    DATASET (DiffString) </xsl:text><xsl:call-template name="output_ecl_name"/>
      </xsl:otherwise>
    </xsl:choose>
    <xsl:text> {xpath('</xsl:text>
    <xsl:if test="not(@flat_array)"><xsl:value-of select="@name"/></xsl:if>
    <xsl:text>/</xsl:text><xsl:call-template name="output_item_tag"/>
    <xsl:text>')</xsl:text>
    <xsl:choose>
      <xsl:when test="@max_count_var"><xsl:text>, MAXCOUNT(</xsl:text><xsl:value-of select="@max_count_var"/><xsl:text> * 2)</xsl:text></xsl:when>
      <xsl:when test="@max_count"><xsl:text>, MAXCOUNT(</xsl:text><xsl:value-of select="@max_count"/><xsl:text> * 2)</xsl:text></xsl:when>
      <xsl:otherwise><xsl:text>, MAXCOUNT(10)</xsl:text></xsl:otherwise>
    </xsl:choose>
    <xsl:text>};</xsl:text><xsl:call-template name="output_comments"/><xsl:text>
</xsl:text>
</xsl:template>

<xsl:template match="EsdlArray[starts-with(@ecl_type,'string') or starts-with(@ecl_type,'unicode')]" mode="layouts">
  <xsl:text>    </xsl:text><xsl:value-of select="@ecl_type"/><xsl:text> </xsl:text><xsl:call-template name="output_ecl_name"/>
  <xsl:text> {xpath('</xsl:text><xsl:value-of select="@name"/><xsl:text>')};</xsl:text><xsl:call-template name="output_comments"/><xsl:text>
</xsl:text>
</xsl:template>

<xsl:template match="EsdlArray" mode="layouts">
    <xsl:text>    dataset(_lt_</xsl:text><xsl:call-template name="output_ecl_array_type"/><xsl:text>) </xsl:text><xsl:call-template name="output_ecl_name"/>
    <xsl:text> {xpath('</xsl:text><xsl:if test="not(@flat_array)"><xsl:value-of select="@name"/></xsl:if><xsl:text>/</xsl:text><xsl:call-template name="output_item_tag"/><xsl:text>')</xsl:text>
    <xsl:choose>
      <xsl:when test="@max_count_var"><xsl:text>, MAXCOUNT(</xsl:text><xsl:value-of select="@max_count_var"/><xsl:text> * 2)</xsl:text></xsl:when>
      <xsl:when test="@max_count"><xsl:text>, MAXCOUNT(</xsl:text><xsl:value-of select="@max_count"/><xsl:text> * 2)</xsl:text></xsl:when>
      <xsl:otherwise><xsl:text>, MAXCOUNT(10)</xsl:text></xsl:otherwise>
    </xsl:choose>
    <xsl:text>};</xsl:text><xsl:call-template name="output_comments"/><xsl:text>
</xsl:text>
</xsl:template>

<xsl:template match="DiffMatchs" mode="layouts">
</xsl:template>

<xsl:template match="_diff_selectors" mode="layouts">
</xsl:template>

<xsl:template match="EsdlStruct" mode="layouts">
    <xsl:text>  EXPORT _lt_</xsl:text><xsl:call-template name="output_ecl_name"/><xsl:text> := RECORD</xsl:text>
    <xsl:call-template name="output_lt_base_type"/>
    <xsl:if test="@max_len"><xsl:text>, MAXLENGTH (</xsl:text><xsl:value-of select="@max_len"/><xsl:text>)</xsl:text></xsl:if>
    <xsl:text>
</xsl:text>
    <xsl:apply-templates select="*" mode="layouts"/>
    <xsl:if test="@element and not(*[@name='Content_'])">    string Content_ {xpath('')};</xsl:if>
    <xsl:text>  END;

</xsl:text>

    <xsl:if test="@_usedInArray and @diff_monitor">
      <xsl:text>  EXPORT _lt_row_</xsl:text><xsl:call-template name="output_ecl_name"/><xsl:text> := RECORD </xsl:text> (_lt_<xsl:call-template name="output_ecl_name"/>)<xsl:text>
    integer _diff_ord {xpath('@diff_ord')} := 0;
  END;

</xsl:text>
    </xsl:if>
</xsl:template>

<xsl:template match="EsdlRequest" mode="layouts">
  <xsl:text>  EXPORT _lt_</xsl:text><xsl:call-template name="output_ecl_name"/><xsl:text> := RECORD</xsl:text>
  <xsl:call-template name="output_lt_base_type"/>
  <xsl:if test="@max_len"><xsl:text>, MAXLENGTH (</xsl:text><xsl:value-of select="@max_len"/><xsl:text>)</xsl:text></xsl:if>
    <xsl:text>
</xsl:text>
  <xsl:apply-templates select="*" mode="layouts"/>
  <xsl:text>  END;

</xsl:text>
    <xsl:if test="@diff_usedInArray and @diff_monitor">
      <xsl:text>  EXPORT _lt_row_</xsl:text><xsl:call-template name="output_ecl_name"/><xsl:text> := RECORD </xsl:text> _lt_<xsl:call-template name="output_ecl_name"/>
      <xsl:text>    integer _diff_ord {xpath('@diff_ord')} := 0;
  END;

</xsl:text>
    </xsl:if>
</xsl:template>

<xsl:template match="EsdlResponse" mode="layouts">
  <xsl:text>  EXPORT _lt_</xsl:text><xsl:call-template name="output_ecl_name"/><xsl:text> := RECORD</xsl:text>
  <xsl:call-template name="output_lt_base_type"/>
  <xsl:if test="@max_len"><xsl:text>, MAXLENGTH (</xsl:text><xsl:value-of select="@max_len"/><xsl:text>)</xsl:text></xsl:if>
  <xsl:text>
</xsl:text>
  <xsl:apply-templates select="*" mode="layouts"/>
  <xsl:text>  END;

</xsl:text>

    <xsl:if test="@diff_usedInArray and @diff_monitor">
      <xsl:text>  EXPORT _lt_row_</xsl:text><xsl:call-template name="output_ecl_name"/><xsl:text> := RECORD </xsl:text> _lt_<xsl:call-template name="output_ecl_name"/>
      <xsl:text>    integer _diff_ord {xpath('@diff_ord')} := 0;
  END;

</xsl:text>
    </xsl:if>
</xsl:template>


<xsl:template match="EsdlArray[starts-with(@ecl_type,'string') or starts-with(@ecl_type,'unicode')]">
    <xsl:text>  boolean </xsl:text><xsl:call-template name="output_ecl_name"/>
    <xsl:text> {xpath('</xsl:text><xsl:value-of select="@name"/><xsl:text>')} := FALSE;
</xsl:text>
</xsl:template>

<!-- common -->

<xsl:template name="output_ecl_name">
<xsl:choose>
  <xsl:when test="@ecl_name"><xsl:value-of select="@ecl_name"/></xsl:when>
  <xsl:otherwise>
    <xsl:variable name="nameword" select="translate(@name, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')"/>
    <xsl:choose>
      <xsl:when test="$nameword='shared'"><xsl:text>_</xsl:text></xsl:when>
    </xsl:choose>
    <xsl:value-of select="@name"/>
  </xsl:otherwise>
</xsl:choose>
</xsl:template>

<xsl:template name="output_basic_type">
  <xsl:param name="basic_type" select="@type"/>
  <xsl:param name="size" select="@max_len"/>
  <xsl:choose>
    <xsl:when test="@ecl_type"><xsl:value-of select="@ecl_type"/><xsl:if test="not(@ecl_max_len)"><xsl:value-of select="$size"/></xsl:if></xsl:when>
    <xsl:when test="$basic_type='int'"><xsl:text>integer</xsl:text><xsl:if test="not(@ecl_max_len)"><xsl:value-of select="$size"/></xsl:if></xsl:when>

    <xsl:when test="$basic_type='unsignedInt'"><xsl:text>unsigned</xsl:text><xsl:if test="not(@ecl_max_len)"><xsl:value-of select="$size"/></xsl:if></xsl:when>
    <xsl:when test="$basic_type='unsignedShort'"><xsl:text>unsigned2</xsl:text></xsl:when>
    <xsl:when test="$basic_type='unsignedByte'"><xsl:text>unsigned1</xsl:text></xsl:when>
    <xsl:when test="$basic_type='long'"><xsl:text>integer4</xsl:text><xsl:if test="not(@ecl_max_len)"><xsl:value-of select="$size"/></xsl:if></xsl:when>
    <xsl:when test="$basic_type='short'"><xsl:text>integer2</xsl:text></xsl:when>
    <xsl:when test="$basic_type='int64'"><xsl:text>integer8</xsl:text></xsl:when>
    <xsl:when test="$basic_type='bool'"><xsl:text>boolean</xsl:text></xsl:when>
    <xsl:when test="$basic_type='string'"><xsl:text>string</xsl:text><xsl:if test="not(@ecl_max_len)"><xsl:value-of select="$size"/></xsl:if></xsl:when>
    <xsl:when test="$basic_type='double'"><xsl:text>real8</xsl:text></xsl:when>
    <xsl:when test="$basic_type='float'"><xsl:text>real4</xsl:text></xsl:when>
    <xsl:when test="$basic_type='base64Binary'"><xsl:text>string</xsl:text></xsl:when>
    <xsl:when test="$basic_type"><xsl:value-of select="$basic_type"/><xsl:if test="not(@ecl_max_len)"><xsl:value-of select="$size"/></xsl:if></xsl:when>
    <xsl:otherwise><xsl:text>string</xsl:text><xsl:if test="not(@ecl_max_len)"><xsl:value-of select="$size"/></xsl:if></xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:template name="output_enum_type">
  <xsl:variable name="etype" select="@enum_type"/>
  <xsl:choose>
    <xsl:when test="/expesdl/types/type[@name=$etype]/@base_type">
      <xsl:call-template name="output_basic_type">
        <xsl:with-param name="basic_type" select="/expesdl/types/type[@name=$etype]/@base_type"/>
      </xsl:call-template>
    </xsl:when>
    <xsl:otherwise>
      <xsl:call-template name="output_basic_type"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:template name="output_lt_base_type">
    <xsl:choose>
      <xsl:when test="@base_type"><xsl:text> (_lt_</xsl:text><xsl:value-of select="@base_type"/><xsl:text>)</xsl:text>
      </xsl:when>
      <xsl:when test="@diff_monitor or @child_mon or @child_mon_base">
  <xsl:text> (DiffMetaRec)</xsl:text>
      </xsl:when>
      <xsl:otherwise></xsl:otherwise>
    </xsl:choose>
</xsl:template>

<xsl:template name="output_ecl_complex_type">
  <xsl:variable name="ctype" select="@complex_type"/>
  <xsl:choose>
    <xsl:when test="@ecl_type"><xsl:value-of select="@ecl_type"/></xsl:when>
    <xsl:otherwise>
      <xsl:text></xsl:text><xsl:value-of select="$ctype"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:template name="output_ecl_array_type">
  <xsl:variable name="ctype" select="@type"/>
  <xsl:choose>
    <xsl:when test="@ecl_type"><xsl:value-of select="@ecl_type"/></xsl:when>
    <xsl:otherwise>
      <xsl:variable name="srcfile" select="/expesdl/types/type[@name=$ctype]/@src"/>
      <xsl:if test="$sourceFileName != $srcfile">
        <xsl:value-of select="$srcfile"/><xsl:text>.</xsl:text>
      </xsl:if>
      <xsl:text></xsl:text><xsl:value-of select="$ctype"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:template name="output_comments">
    <xsl:if test="@ecl_comment"><xsl:value-of select="@ecl_comment"/></xsl:if>
    <xsl:choose>
        <xsl:when test="@complex_type">
            <xsl:variable name="ctype" select="@complex_type"/>
            <xsl:if test="/expesdl/types/type[@name=$ctype]/@comment">
                <xsl:text>//</xsl:text><xsl:value-of select="/expesdl/types/type[@name=$ctype]/@comment"/>
            </xsl:if>
        </xsl:when>
        <xsl:when test="@enum_type">
            <xsl:variable name="etype" select="@enum_type"/>
            <xsl:if test="/expesdl/types/type[@name=$etype]/@comment">
                <xsl:text>//</xsl:text><xsl:value-of select="/expesdl/types/type[@name=$etype]/@comment"/>
            </xsl:if>
        </xsl:when>
    </xsl:choose>
    <xsl:if test="@optional">
        <xsl:text>//hidden[</xsl:text><xsl:value-of select="@optional"/><xsl:text>]</xsl:text>
    </xsl:if>
    <xsl:if test="@ecl_type and (@type or @complex_type)">
        <xsl:choose>
         <xsl:when test="name()='EsdlArray'">
           <xsl:text> // Real type: </xsl:text>
           <xsl:text>dataset(</xsl:text>
           <xsl:value-of select="@type"/>
           <xsl:text>) </xsl:text>
           <xsl:call-template name="output_ecl_name"/>
           <xsl:text> {xpath('</xsl:text>
           <xsl:value-of select="@name"/>
           <xsl:text>/</xsl:text>
           <xsl:call-template name="output_item_tag"/>
           <xsl:text>')};</xsl:text>
         </xsl:when>
         <xsl:when test="name()='EsdlElement' and starts-with(@ecl_type,'tns:')">
           <xsl:text> // Real type: RECORD </xsl:text>
           <xsl:value-of select="@type|@complex_type"/>
         </xsl:when>
         <xsl:when test="name()='EsdlElement' and not(starts-with(@ecl_type,'tns:'))">
           <xsl:text> // Xsd type: </xsl:text>
           <xsl:value-of select="@type|@complex_type"/>
         </xsl:when>
         <xsl:otherwise>
             <xsl:value-of select="@type"/>
         </xsl:otherwise>
        </xsl:choose>
    </xsl:if>
</xsl:template>

<xsl:template name="output_xpath">
    <xsl:text>xpath('</xsl:text>
    <xsl:choose>
        <xsl:when test="@ecl_path"><xsl:value-of select="@ecl_path"/></xsl:when>
        <xsl:when test="@get_data_from"><xsl:if test="@attribute"><xsl:value-of select="'@'"/></xsl:if> <xsl:value-of select="@get_data_from"/></xsl:when>
        <xsl:when test="@alt_data_from"><xsl:if test="@attribute"><xsl:value-of select="'@'"/></xsl:if> <xsl:value-of select="@alt_data_from"/></xsl:when>
        <xsl:otherwise><xsl:if test="@attribute"><xsl:value-of select="'@'"/></xsl:if> <xsl:value-of select="@name"/></xsl:otherwise>
    </xsl:choose>
    <xsl:text>')</xsl:text>
</xsl:template>

<xsl:template name="output_item_tag">
    <xsl:choose>
         <xsl:when test="@item_tag"><xsl:value-of select="@item_tag"/></xsl:when>
         <xsl:when test="@type and (@type='int' or @type='integer' or @type='bool' or @type='short' or @type='float' or @type='double' or @type='string' or @type='long' or @type='decimal' or @type='byte' or @type='unsignedInt' or @type='unsignedShort' or @type='unsignedByte')">
           <xsl:value-of select="'Item'"/>
         </xsl:when>
         <xsl:when test="@type">
           <xsl:value-of select="@type"/>
         </xsl:when>
         <xsl:otherwise><xsl:value-of select="'Item'"/></xsl:otherwise>
    </xsl:choose>
</xsl:template>

<xsl:template name="output_active_check">
  <xsl:param name="pathvar"/>
  <xsl:param name="def" select="'is_active'"/>
  <xsl:choose>
    <xsl:when test="diff_selectors/entry">
    <xsl:text>CASE(</xsl:text><xsl:value-of select="$pathvar"/><xsl:text>, </xsl:text>
  <xsl:for-each select="diff_selectors/entry">
    <xsl:if test="position()!=1"><xsl:text>, </xsl:text>
    </xsl:if>
    <xsl:text>'/</xsl:text><xsl:value-of select="@path"/><xsl:text>' => (</xsl:text>
  <xsl:for-each select="category">
    <xsl:if test="position()!=1"><xsl:text> OR </xsl:text></xsl:if><xsl:value-of select="."/>
  </xsl:for-each><xsl:text>)</xsl:text>
      </xsl:for-each>, <xsl:value-of select="$def"/><xsl:text>)</xsl:text>
    </xsl:when>
    <xsl:otherwise><xsl:value-of select="$def"/></xsl:otherwise>
  </xsl:choose>
</xsl:template>

</xsl:stylesheet>
