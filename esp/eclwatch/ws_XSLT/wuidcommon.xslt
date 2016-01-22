<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    This program is free software: you can redistribute it and/or modify
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

<!DOCTYPE xsl:stylesheet [
  <!ENTITY nbsp "&#160;">
]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="html"/>
  <xsl:template match="Workunit">
    <form name="protect" id="protect" action="/WsWorkunits/WUUpdate?Wuid={$wuid}" method="post">
      <div id="wudetails" xmlns="http://www.w3.org/1999/xhtml">
        <table id="wudetailsContent" class="workunit">
          <xsl:text disable-output-escaping="yes"><![CDATA[        
        <colgroup>
          <col width="30%"/>
          <col width="70%"/>
        </colgroup>
        ]]></xsl:text>

          <tr>
            <td class="eclwatch entryprompt">
              WUID:
            </td>
            <td>
              <xsl:choose>
                <xsl:when test="number(Archived)">
                  <xsl:value-of select="$wuid"/>
                </xsl:when>
                <xsl:otherwise>
                  <xsl:value-of select="$wuid"/>
                  &nbsp;
                  <a href="/esp/iframe?esp_iframe_title=ECL Workunit XML - {$wuid}&amp;inner=/WsWorkunits/WUFile%3fWuid%3d{$wuid}%26Type%3dXML%26Option%3d0%26SizeLimit%3d5000000" >XML</a>
                  &nbsp;
                  <a href="/esp/iframe?esp_iframe_title=Download ECL Workunit XML - {$wuid}&amp;inner=/WsWorkunits/WUFile%3fWuid%3d{$wuid}%26Type%3dXML%26Option%3d2" >Download XML</a>
                  &nbsp;
                  <a href="/esp/iframe?esp_iframe_title=ECL Playground - {$wuid}&amp;inner=/esp/files/stub.htm%3fWidget%3dECLPlaygroundWidget%26Wuid%3d{$wuid}%26Target%3d{Cluster}" >ECL Playground</a>
                </xsl:otherwise>
              </xsl:choose>
            </td>
          </tr>
          <tr>
            <td class="eclwatch entryprompt">
              Action:
            </td>
            <td>
              <xsl:value-of select="ActionEx"/>
            </td>
          </tr>
          <tr>
            <td class="eclwatch entryprompt">
              State:
            </td>
            <td>
              <xsl:if test="State!='failed'">
                <xsl:choose>
                  <xsl:when test="number(Archived)">
                    <xsl:value-of select="State"/>
                  </xsl:when>
                  <xsl:when test="number(IsPausing)">
                    Pausing
                  </xsl:when>
                  <xsl:otherwise>
                    <select size="1" name="State">
                      <xsl:choose>
                        <xsl:when test="State='unknown'">
                          <option value="0" selected="selected">
                            <xsl:value-of select="State"/>
                          </option>
                        </xsl:when>
                        <xsl:when test="State='compiled'">
                          <option value="1" selected="selected">
                            <xsl:value-of select="State"/>
                          </option>
                        </xsl:when>
                        <xsl:when test="State='running'">
                          <option value="2" selected="selected">
                            <xsl:value-of select="State"/>
                          </option>
                        </xsl:when>
                        <xsl:when test="State='completed'">
                          <option value="3" selected="selected">
                            <xsl:value-of select="State"/>
                          </option>
                        </xsl:when>
                        <xsl:when test="State='archived'">
                          <option value="5" selected="selected">
                            <xsl:value-of select="State"/>
                          </option>
                        </xsl:when>
                        <xsl:when test="State='aborting'">
                          <option value="6" selected="selected">
                            <xsl:value-of select="State"/>
                          </option>
                        </xsl:when>
                        <xsl:when test="State='aborted'">
                          <option value="7" selected="selected">
                            <xsl:value-of select="State"/>
                          </option>
                        </xsl:when>
                        <xsl:when test="State='blocked'">
                          <option value="8" selected="selected">
                            <xsl:value-of select="State"/>
                          </option>
                        </xsl:when>
                        <xsl:when test="State='submitted'">
                          <option value="9" selected="selected">
                            <xsl:value-of select="State"/>
                          </option>
                        </xsl:when>
                        <xsl:when test="State='scheduled'">
                          <xsl:choose>
                            <xsl:when test="number(Aborting)">
                              <option value="10" selected="selected">scheduled(aborting)</option>
                            </xsl:when>
                            <xsl:otherwise>
                              <option value="10" selected="selected">
                                <xsl:value-of select="State"/>
                              </option>
                            </xsl:otherwise>
                          </xsl:choose>
                        </xsl:when>
                        <xsl:when test="State='compiling'">
                          <option value="11" selected="selected">
                            <xsl:value-of select="State"/>
                          </option>
                        </xsl:when>
                        <xsl:when test="State='wait'">
                          <option value="12" selected="selected">
                            <xsl:value-of select="State"/>
                          </option>
                        </xsl:when>
                        <xsl:when test="State='uploading_files'">
                          <option value="13" selected="selected">
                            <xsl:value-of select="State"/>
                          </option>
                        </xsl:when>
                        <xsl:when test="State='debugging'">
                          <option value="14" selected="selected">
                            <xsl:value-of select="State"/>
                          </option>
                        </xsl:when>
                        <xsl:when test="State='debug_running'">
                          <option value="15" selected="selected">
                            <xsl:value-of select="State"/>
                          </option>
                        </xsl:when>
                        <xsl:when test="State='paused'">
                          <option value="16" selected="selected">
                            <xsl:value-of select="State"/>
                          </option>
                        </xsl:when>
                      </xsl:choose>
                      <option value="4">failed</option>
                    </select>
                  </xsl:otherwise>
                </xsl:choose>
              </xsl:if>
              <xsl:if test="State='failed'">
                failed
              </xsl:if>
              <xsl:if test="string-length(StateEx)">
                <span>
                  <xsl:value-of select="StateEx"/>
                </span>
              </xsl:if>
            </td>
          </tr>
          <tr>
            <td class="eclwatch entryprompt">
              Owner:
            </td>
            <td>
              <xsl:value-of select="Owner"/>
            </td>
          </tr>
          <xsl:if test="$SecMethod='LdapSecurity'">
            <tr>
              <td class="eclwatch entryprompt">
                Scope:
              </td>
              <td>
                <xsl:choose>
                  <xsl:when test="State='running'">
                    <input type="text" name="Scope" value="{Scope}" size="40" disabled="disabled"/>
                  </xsl:when>
                  <xsl:otherwise>
                    <input type="text" name="Scope" value="{Scope}" size="40"/>
                  </xsl:otherwise>
                </xsl:choose>
              </td>
            </tr>
          </xsl:if>
          <tr>
              <td colspan="2">
                <div style="border:1px solid grey;">
                  <br />
                  <span class="eclwatch entryprompt">Jobname:&#160;</span>
                  <xsl:choose>
                    <xsl:when test="State='running'">
                      <input type="text" name="Jobname" id="Jobname" value="{Jobname}" size="40" disabled="disabled"/>
                    </xsl:when>
                    <xsl:otherwise>
                      <input type="text" name="Jobname" id="Jobname" value="{Jobname}" size="40"/>
                    </xsl:otherwise>
                  </xsl:choose>
                  <input type="button" name="Publish" value="Publish" class="sbutton" onclick="publishWorkunit('{$wuid}');" />
                  <br />
                </div>
              </td>
            </tr>
          <tr>
            <td class="eclwatch entryprompt">
              Description:
            </td>
            <td>
              <xsl:choose>
                <xsl:when test="State='running'">
                  <input type="text" name="Description" value="{Description}" size="40" disabled="disabled"/>
                </xsl:when>
                <xsl:otherwise>
                  <input type="text" name="Description" value="{Description}" size="40"/>
                </xsl:otherwise>
              </xsl:choose>
            </td>
          </tr>
          <tr>
            <td class="eclwatch entryprompt">
              Protected:
            </td>
            <td>
              <xsl:choose>
                <xsl:when test="number(Archived) or number(AccessFlag) &lt; 7">
                  <xsl:choose>
                    <xsl:when test="number(Protected)">Yes</xsl:when>
                    <xsl:otherwise>No</xsl:otherwise>
                  </xsl:choose>
                </xsl:when>
                <xsl:otherwise>
                  <input type="checkbox" name="Protected" onclick="setProtected(this);">
                    <xsl:if test="number(Protected)">
                      <xsl:attribute name="checked"/>
                    </xsl:if>
                  </input>
                </xsl:otherwise>
              </xsl:choose>
            </td>
          </tr>
          <xsl:variable name="defaultcluster" select="Cluster"/>
          <tr>
            <td class="eclwatch entryprompt">
              Cluster:
            </td>
            <td>
              <xsl:choose>
                <xsl:when test="count(AllowedClusters/AllowedCluster) > 1">
                  <select name="ClusterSelection" id="ClusterSelection" size="1">
                    <xsl:for-each select="AllowedClusters/AllowedCluster">
                      <option>
                        <xsl:if test=".=$defaultcluster">
                          <xsl:attribute name="selected">true</xsl:attribute>
                        </xsl:if>
                        <xsl:value-of select="."/>
                      </option>
                    </xsl:for-each>
                  </select>
                </xsl:when>
                <xsl:when test="count(AllowedClusters/AllowedCluster) > 0">
                  <xsl:value-of select="AllowedClusters/AllowedCluster[1]"/>
                </xsl:when>
                <xsl:otherwise>
                  <xsl:value-of select="Cluster"/>
                </xsl:otherwise>
              </xsl:choose>
            </td>
          </tr>
        </table>
      </div>
      
      <xsl:if test="HasDebugValue &gt; 0 or DebugValuesDesc != ''">
        <p>
          <div>
            <div class="wugroup">
              <div class="WuGroupHdrLeft">
                <A href="javascript:void(0)" onclick="toggleElement('DebugValues');" id="explinkdebugvalues" class="wusectionexpand">
                  Complexity:
                </A>
              </div>
            </div>
            <xsl:choose>
              <xsl:when test="DebugValuesDesc != ''">
                <div id="DebugValuesContent">
                  <xsl:value-of select="DebugValuesDesc"/>
                </div>
              </xsl:when>
              <xsl:otherwise>
                <div id="DebugValues" class="wusectioncontent">
                  <xsl:if test="count(DebugValues/DebugValue[Name='__calculated__complexity__'])">
                    <table id="DebugValuesContent" class="wusectiontable">
                      <xsl:value-of select="DebugValues/DebugValue[Name='__calculated__complexity__']/Value"/>
                    </table>
                  </xsl:if>
                </div>
              </xsl:otherwise>
            </xsl:choose>
          </div>
        </p>
      </xsl:if>

      <xsl:if test="ErrorCount &gt; 0">
        <p>
          <div>
            <div id="HdrExceptions" class="wugroup">
              <div class="WuGroupHdrLeft">
                <A href="javascript:void(0)" onclick="toggleElement('Exceptions');" id="explinkexceptions" class="wusectionexpand">
                  Errors:&nbsp;
                    (<xsl:value-of select="ErrorCount"/>)
                </A>
              </div>
            </div>
            <div id="Exceptions" class="wusectioncontent">
              <xsl:if test="count(Exceptions/ECLException[Severity='Error'])=0">
                <span class="loading">&nbsp;&nbsp;Loading...</span>
              </xsl:if>
              <xsl:if test="count(Exceptions/ECLException[Severity='Error'])">
                <table id="ExceptionsContent" class="wusectiontable">
                  <xsl:apply-templates select="Exceptions/ECLException[Severity='Error']"/>
                </table>
              </xsl:if>
            </div>
          </div>
        </p>
      </xsl:if>
      
      <xsl:if test="WarningCount &gt; 0">
        <p>
          <div>
            <div id="HdrWarnings" class="wugroup">
              <div class="WuGroupHdrLeft">
                <A href="javascript:void(0)" onclick="toggleElement('Warnings');" id="explinkwarnings" class="wusectionexpand">
                  Warnings:&nbsp;(<xsl:value-of select="WarningCount"/>)
                </A>
              </div>
            </div>
            <div id="Warnings" class="wusectioncontent">
              <xsl:if test="count(Exceptions/ECLException[Severity='Warning'])=0">
                <span class="loading">&nbsp;&nbsp;Loading...</span>
              </xsl:if>
              <xsl:if test="count(Exceptions/ECLException[Severity='Warning'])">
                <table id="WarningsContent" class="wusectiontable">
                  <xsl:apply-templates select="Exceptions/ECLException[Severity='Warning']"/>
                </table>
              </xsl:if>
            </div>
          </div>
        </p>
      </xsl:if>
      
      <xsl:if test="InfoCount &gt; 0">
        <p>
          <div>
            <div id="HdrInfo" class="wugroup">
              <div class="WuGroupHdrLeft">
                <A href="javascript:void(0)" onclick="toggleElement('Info');" id="explinkinfo" class="wusectionexpand">
                  Info: (<xsl:value-of select="InfoCount"/>)
                </A>
              </div>
            </div>
            <div id="Info" class="wusectioncontent">
              <xsl:if test="count(Exceptions/ECLException[Severity='Info'])=0">
                <span class="loading">&nbsp;&nbsp;Loading...</span>
              </xsl:if>
              <xsl:if test="count(Exceptions/ECLException[Severity='Info'])">
                <table id="InfoContent" class="wusectiontable">
                  <xsl:apply-templates select="Exceptions/ECLException[Severity='Info']"/>
                </table>
              </xsl:if>
            </div>
          </div>
        </p>
      </xsl:if>
      
      <xsl:if test="AlertCount &gt; 0">
        <p>
          <div>
            <div id="HdrAlert" class="wugroup">
              <div class="WuGroupHdrLeft">
                <A href="javascript:void(0)" onclick="toggleElement('Alert');" id="explinkalert" class="wusectionexpand">
                  Alert: (<xsl:value-of select="AlertCount"/>)
                </A>
              </div>
            </div>
            <div id="Alert" class="wusectioncontent">
              <xsl:if test="count(Exceptions/ECLException[Severity='Alert'])=0">
                <span class="loading">&nbsp;&nbsp;Loading...</span>
              </xsl:if>
              <xsl:if test="count(Exceptions/ECLException[Severity='Alert'])">
                <table id="AlertContent" class="wusectiontable">
                  <xsl:apply-templates select="Exceptions/ECLException[Severity='Alert']"/>
                </table>
              </xsl:if>
            </div>
          </div>
        </p>
      </xsl:if>
      
      <xsl:if test="ResultCount &gt; 0 or ResultsDesc != ''">
        <p>
          <div>
            <div class="wugroup">
              <div class="WuGroupHdrLeft">
                <A href="javascript:void(0)" onclick="toggleElement('Results');" id="explinkresults" class="wusectionexpand">
                  Results: (<xsl:value-of select="ResultCount"/>)
                </A>
                &nbsp;-&nbsp;
                <a href="/esp/iframe?esp_iframe_title=Results - {$wuid}&amp;inner=/esp/files/stub.htm%3fWidget%3dResultsWidget%26Wuid%3d{$wuid}%26TabPosition%3dtop" >Show</a>
              </div>
            </div>
            <div id="Results" class="wusectioncontent">
              <xsl:choose>
                <xsl:when test="ResultsDesc != ''">
                  <div id="ResultsContent">
                    <xsl:value-of select="ResultsDesc"/>
                  </div>
                </xsl:when>
                <xsl:otherwise>
                  <xsl:choose>
                    <xsl:when test="string-length(ResultsDesc)">
                      <xsl:value-of select="ResultsDesc"/>
                    </xsl:when>
                    <xsl:otherwise>
                      <xsl:if test="count(Results/ECLResult)=0">
                        <span class="loading">&nbsp;&nbsp;Loading...</span>
                      </xsl:if>
                      <xsl:if test="count(Results/ECLResult)">
                        <table id="ResultsContent" class="wusectiontable">
                          <colgroup>
                            <col width="30%"/>
                            <col width="125px"/>
                            <col/>
                          </colgroup>
                          <xsl:apply-templates select="Results" />
                        </table>
                      </xsl:if>
                    </xsl:otherwise>
                  </xsl:choose>
                </xsl:otherwise>
              </xsl:choose>
            </div>
          </div>
        </p>
      </xsl:if>
      
      <xsl:if test="SourceFileCount &gt; 0 or SourceFilesDesc != ''">
        <p>
          <div>
            <div class="wugroup" onclick="toggleElement('');">
              <div class="WuGroupHdrLeft">
                <A href="javascript:void(0)" onclick="toggleElement('SourceFiles');" id="explinksourcefiles" class="wusectionexpand">
                  Files: (<xsl:value-of select="SourceFileCount"/>)
                </A>
              </div>
            </div>

            <div id="SourceFiles" class="wusectioncontent">
              <xsl:choose>
                <xsl:when test="SourceFilesDesc != ''">
                  <div id="SourceFilesContent">
                    <xsl:value-of select="SourceFilesDesc"/>
                  </div>
                </xsl:when>
                <xsl:otherwise>
                  <xsl:if test="count(SourceFiles/ECLSourceFile)=0">
                    <span class="loading">&nbsp;&nbsp;Loading...</span>
                  </xsl:if>
                  <xsl:if test="count(SourceFiles/ECLSourceFile)">
                    <table id="SourceFilesContent" class="wusectiontable">
                      <colgroup>
                        <col width="5px"/>
                        <col/>
                      </colgroup>
                      <thead>
                        <tr>
                          <td></td>
                          <td>SourceFile</td>
                        </tr>
                      </thead>
                      <tbody>
                        <xsl:apply-templates select="SourceFiles/ECLSourceFile"/>
                      </tbody>
                    </table>
                  </xsl:if>
                </xsl:otherwise>
              </xsl:choose>
            </div>
          </div>
        </p>
      </xsl:if>

      <xsl:if test="VariableCount &gt; 0 or VariablesDesc != ''">
        <p>
          <div>
            <div class="wugroup">
              <div class="WuGroupHdrLeft">
                <A href="javascript:void(0)" onclick="toggleElement('Variables');" id="explinkvariables" class="wusectionexpand">
                  Variables: (<xsl:value-of select="VariableCount"/>)
                </A>
              </div>
            </div>
            <xsl:choose>
              <xsl:when test="VariablesDesc != ''">
                <div id="VariablesContent">
                  <xsl:value-of select="VariablesDesc"/>
                </div>
              </xsl:when>
              <xsl:otherwise>
                <div id="Variables" class="wusectioncontent">
                  <xsl:if test="count(Variables/ECLResult)=0">
                    <span class="loading">&nbsp;&nbsp;Loading...</span>
                  </xsl:if>
                  <xsl:if test="count(Variables/ECLResult)">
                    <table id="VariablesContent" class="wusectiontable">
                      <xsl:apply-templates select="Variables"/>
                    </table>
                  </xsl:if>
                </div>
              </xsl:otherwise>
            </xsl:choose>
          </div>
        </p>
      </xsl:if>
      
      <xsl:if test="GraphCount &gt; 0 or GraphsDesc != ''">
        <p>
          <div>
            <div class="wugroup">
              <div class="WuGroupHdrLeft">
                <A href="javascript:void(0)" onclick="toggleElement('Graphs');" id="explinkgraphs" class="wusectionexpand">
                  Graphs: (<xsl:value-of select="GraphCount"/>)
                </A>
              </div>
            </div>
          </div>
          <div id="Graphs" class="wusectioncontent">
            <xsl:choose>
              <xsl:when test="GraphsDesc != ''">
                <div id="GraphsContent">
                  <xsl:value-of select="GraphsDesc"/>
                </div>
              </xsl:when>
              <xsl:otherwise>
                <xsl:if test="count(Graphs/ECLGraph)=0">
                  <span class="loading">&nbsp;&nbsp;Loading...</span>
                </xsl:if>
                <xsl:if test="count(Graphs/ECLGraph)">
                  <table id="GraphsContent" class="wusectiontable">
                    <colgroup>
                      <col/>
                      <col/>
                    </colgroup>
                    <tr>
                      <td colspan="2">
                        <xsl:if test="$havesubgraphtimings=1">
                          <a style="align:right;" href="/WsWorkunits/WUGraphTiming?Wuid={$wuid}" >Sub Graph Timings</a>
                        </xsl:if>
                      </td>
                    </tr>
                    <xsl:apply-templates select="Graphs"/>
                  </table>
                </xsl:if>
              </xsl:otherwise>
            </xsl:choose>
          </div>
        </p>
      </xsl:if>
      
      <xsl:if test="TimerCount &gt; 0 or TimersDesc != ''">
        <p>
          <div>
            <div class="wugroup">
              <div class="WuGroupHdrLeft">
                <A href="javascript:void(0)" onclick="toggleElement('Timers');" id="explinktimers" class="wusectionexpand">
                  Timings: (<xsl:value-of select="TimerCount"/>)
                </A>
              </div>
            </div>
            <div id="Timers" class="wusectioncontent">
              <xsl:choose>
                <xsl:when test="TimersDesc != ''">
                  <div id="TimersContent">
                    <xsl:value-of select="TimersDesc"/>
                  </div>
                </xsl:when>
                <xsl:otherwise>
                  <xsl:if test="count(Timers/ECLTimer)=0">
                    <span class="loading">&nbsp;&nbsp;Loading...</span>
                  </xsl:if>
                  <xsl:if test="count(Timers/ECLTimer)">
                    <table id="TimersContent" class="wusectiontable" cellspacing="0px">
                      <colgroup>
                        <col width="40%"/>
                        <col align="char" char="."/>
                      </colgroup>
                      <xsl:apply-templates select="Timers" />
                    </table>
                  </xsl:if>
                </xsl:otherwise>
              </xsl:choose>
            </div>
          </div>
        </p>
      </xsl:if>

      <xsl:if test="string-length(Query/Text) or string-length(Query/QueryMainDefinition)">
        <div>
          <div class="wugroup">
              <div class="WuGroupHdrLeft">
                <A href="javascript:void(0)" onclick="toggleElement('querysection');" id="explinkquerysection" class="wusectionexpand">Query: (1)</A>
              </div>
          </div>
          <div id="querysection" class="wusectioncontent">
              <xsl:if test="string-length(Query/Text)">
                  <div>
                      <textarea id="query" readonly="true" wrap="off" rows="10" STYLE="width:600">
                          <xsl:value-of select="Query/Text"/>
                      </textarea>
                  </div>
              </xsl:if>
              <xsl:if test="string-length(Query/QueryMainDefinition)">
                  <div>
                      <b>QueryMainDefinition: </b><xsl:value-of select="Query/QueryMainDefinition"/>
                  </div>
              </xsl:if>
          </div>
        </div>
      </xsl:if>
      <xsl:if test="HasArchiveQuery != 0">
        <tr>
          <td></td>
          <td>
            <a href="javascript:void(0)" onclick="getOptions('ArchiveQuery', '/WsWorkunits/WUFile/ArchiveQuery?Wuid={$wuid}&amp;Name=ArchiveQuery&amp;Type=ArchiveQuery', true); return false;">
              Archive Query
            </a>
          </td>
        </tr>
      </xsl:if>
      <xsl:if test="WorkflowCount &gt; 0  or WorkflowsDesc != ''">
        <p>
          <div>
            <div class="wugroup">
              <div class="WuGroupHdrLeft">
                <A href="javascript:void(0)" onclick="toggleElement('Workflows');" id="explinkWorkflows" class="wusectionexpand">
                  Workflows:&nbsp;(<xsl:value-of select="WorkflowCount"/>)
                </A>
              </div>
            </div>
            <div id="Workflows" class="wusectioncontent">
              <xsl:choose>
                <xsl:when test="WorkflowsDesc != ''">
                  <div id="WorkflowsContent">
                    <xsl:value-of select="WorkflowsDesc"/>
                  </div>
                </xsl:when>
                <xsl:otherwise>
                  <xsl:if test="count(Workflows/ECLWorkflow)=0">
                    <span class="loading">&nbsp;&nbsp;Loading...</span>
                  </xsl:if>
                  <xsl:if test="Workflows/ECLWorkflow[1]">
                    <table id="WorkflowsContent" class="WorkflowTable">
                      <colgroup>
                        <col width="5"/>
                        <col width="120"/>
                        <col width="120"/>
                        <col width="120"/>
                        <col width="120"/>
                      </colgroup>
                      <thead>
                        <tr>
                          <th class="WorkflowHeader">WFID</th>
                          <th class="WorkflowHeader">Event Name</th>
                          <th class="WorkflowHeader">Event Text</th>
                          <th class="WorkflowHeader">Count</th>
                          <th class="WorkflowHeader">Count Remaining</th>
                        </tr>
                      </thead>
                      <tbody>
                        <xsl:for-each select="Workflows/ECLWorkflow">
                          <tr>
                            <td>
                              <xsl:value-of select="WFID"/>
                            </td>
                            <td>
                              <xsl:value-of select="EventName"/>
                            </td>
                            <td>
                              <xsl:value-of select="EventText"/>
                            </td>
                            <td>
                              <xsl:if test="number(Count) > -1">
                                <xsl:value-of select="Count"/>
                              </xsl:if>
                            </td>
                            <td>
                              <xsl:if test="number(CountRemaining) > -1">
                                <xsl:value-of select="CountRemaining"/>
                              </xsl:if>
                            </td>
                          </tr>
                        </xsl:for-each>
                      </tbody>
                    </table>
                  </xsl:if>
                </xsl:otherwise>
              </xsl:choose>
            </div>
          </div>
        </p>
      </xsl:if>

      <xsl:if test="number(Archived) &lt; 1">
        <xsl:if test="count(ResourceURLs/URL)">
          <p>
            <div class="wugroup">
              <div class="WuGroupHdrLeft">
                <A href="javascript:void(0)" onclick="toggleElement('Resources');" id="explinkResources" class="wusectionexpand">
                  Resources: (<xsl:value-of select="count(ResourceURLs/URL)"/>)
                </A>
              </div>
            </div>
            <div id="Resources" class="wusectioncontent">
              <table id="ResourceTable" class="wusectiontable">
                <xsl:apply-templates select="ResourceURLs"/>
              </table>
            </div>
          </p>
        </xsl:if>
        <xsl:if test="count(Helpers/ECLHelpFile)">
          <p>
            <div class="wugroup">
              <div class="WuGroupHdrLeft">
                <A href="javascript:void(0)" onclick="toggleElement('Helpers');" id="explinkhelpers" class="wusectionexpand">
                  Helpers: (<xsl:value-of select="count(Helpers/ECLHelpFile)"/>)
                </A>
              </div>
            </div>
            <div id="Helpers" class="wusectioncontent">
              <table id="HelperTable" class="wusectiontable">
                <xsl:apply-templates select="Helpers"/>
              </table>
            </div>
          </p>
        </xsl:if>
        <xsl:if test="HelpersDesc != ''">
          <div id="HelpersContent">
            <xsl:value-of select="HelpersDesc"/>
          </div>
        </xsl:if>

        <xsl:if test="(number(ClusterFlag)=1) and (count(ThorLogList/ThorLogInfo) > 0)">
            <table class="workunit">
                <colgroup>
                    <col width="20%"/>
                    <col width="80%"/>
                </colgroup>
                <tr>
                    <td colspan="3">
                        <div style="border:1px solid grey;">
                            <input id="getthorslavelog" type="button" value="Get slave log" onclick="GetThorSlaveLog()"> </input>
                            <xsl:choose>
                                <xsl:when test="number(ThorLogList/ThorLogInfo[1]/NumberSlaves) != 0">
                                    Thor Process: <select id="ThorProcess" name="ThorProcess" onchange="thorProcessChanged(options[selectedIndex].value)">
                                    <xsl:for-each select="ThorLogList/ThorLogInfo">
                                        <xsl:variable name="val">
                                            <xsl:value-of select="./NumberSlaves"/>@<xsl:value-of select="./LogDate"/>@<xsl:value-of select="./ProcessName"/>@<xsl:value-of select="./ClusterGroup"/>
                                        </xsl:variable>
                                        <option value="{$val}">
                                            <xsl:value-of select="./ProcessName"/>
                                        </option>
                                    </xsl:for-each>
                                    </select>
                                    Slave Number<span id="NumberSlaves"></span>: <input type="text" id="SlaveNum" name="SlaveNum" value="1" size="4" onkeypress="return CheckSlaveNum(event);"/>
                                </xsl:when>
                                <xsl:otherwise>
                                    <input type="hidden" id="ProcessName" value="{ThorLogList/ThorLogInfo[1]/ProcessName}"/>
                                    <input type="hidden" id="LogDate" value="{ThorLogList/ThorLogInfo[1]/LogDate}"/>
                                    on: <input type="text" id="SlaveAddress" name="SlaveAddress" title="Type in NetworkAddress or NetworkAddress_Port where the slave run" value="" size="16" onkeypress="return CheckSlaveAddress(event);"/>
                                </xsl:otherwise>
                            </xsl:choose>
                        </div>
                    </td>
                </tr>
            </table>
            <br/>
        </xsl:if>
        <table class="workunit">
          <colgroup>
            <col width="1%"/>
            <col width="99%"/>
          </colgroup>
          <tr>
                <td></td>
            <td>
              <input type="hidden" name="Wuid" value="{$wuid}"/>
              <input type="hidden" name="StateOrig" value="{$state}"/>
              <input type="hidden" name="JobnameOrig" value="{Jobname}"/>
              <input type="hidden" name="ScopeOrig" value="{Scope}"/>
              <input type="hidden" name="DescriptionOrig" value="{Description}"/>
              <input type="hidden" name="ClusterOrig" value="{Cluster}"/>
              <input type="hidden" name="ProtectedOrig" value="{Protected}"/>
              <input type="hidden" id="ESPIPAddress" name="ESPIPAddress" value=""/>
              <input type="hidden" id="ThorIPAddress" name="ThorIPAddress" value=""/>
              <input type="hidden" id="BuildVersion" name="BuildVersion" value=""/>
              <input type="hidden" id="ProblemDescription" name="ProblemDescription" value=""/>
              <input type="hidden" id="WhatChanged" name="WhatChanged" value=""/>
              <input type="hidden" id="WhereSlow" name="WhereSlow" value=""/>
              <input type="hidden" id="Password" name="Password" value=""/>
              <input type="hidden" id="IncludeThorSlaveLog" name="IncludeThorSlaveLog" value="0"/>
              <input type="button" name="Type" value="Save" class="sbutton" onclick="updateWorkunit('{$wuid}');">
                        <xsl:if test="number(AccessFlag) &lt; 7">
                          <xsl:attribute name="disabled">disabled</xsl:attribute>
                        </xsl:if>
                  </input>
              <input type="reset"  name="Type" value="Reset" class="sbutton">
                  <xsl:if test="number(AccessFlag) &lt; 7">
                          <xsl:attribute name="disabled">disabled</xsl:attribute>
                        </xsl:if>
                      </input>
              <input type="button" name="ZAPReport" style="width: 120px" value="Z.A.P. Report" class="sbutton" onclick="return popupZAPInfoForm()"/>
            </td>
          </tr>
          <tr>
            <td colspan="2">
              <div id="UpdateAction">&#160;</div>
            </td></tr>
        </table>
      </xsl:if>
    </form>       
   <div id="wufooterContent">
   <div id="wufooter">
    <xsl:choose>
      <xsl:when test="number(Archived)">
        <form action="/WsWorkunits/WUAction?PageFrom=WUID" method="post">
          <input type="hidden" name="Wuids_i1" value="{$wuid}"/>
          <input type="submit" class="sbutton" name="WUActionType" id="restoreBtn" value="Restore"/>
        </form>
      </xsl:when>
        <xsl:when test="number(ClusterFlag)=2">
        <table>
          <tr>
                <td></td>
            <td>
              <form action="/WsWorkunits/WUAction?PageFrom=WUID" method="post">
                <xsl:variable name="ScheduledAborting">
                  <xsl:choose>
                    <xsl:when test="State='scheduled' and number(Aborting)">1</xsl:when>
                    <xsl:otherwise>0</xsl:otherwise>
                  </xsl:choose>
                </xsl:variable>
                <input type="hidden" name="Wuids_i1" value="{$wuid}"/>
                <input type="submit" name="WUActionType" value="Abort" class="sbutton" title="Abort workunit" onclick="return confirm('Abort workunit?')">
                  <xsl:if test="number(AccessFlag) &lt; 7 or State='aborting' or State='aborted' or State='failed' or State='completed' or $ScheduledAborting!=0">
                    <xsl:attribute name="disabled">disabled</xsl:attribute>
                  </xsl:if>
                </input>
                <input type="submit" name="WUActionType" value="Delete" class="sbutton" title="Delete workunit" onclick="return confirm('Delete workunit?')">
                  <xsl:if test="number(AccessFlag) &lt; 7 or number(Protected)">
                    <xsl:attribute name="disabled">disabled</xsl:attribute>
                  </xsl:if>
                </input>
              </form>
            </td>
          </tr>
        </table>
      </xsl:when>
      <xsl:otherwise>
        <table>
          <tr>
            <td></td>
            <xsl:if test="number(ThorLCR)">
                <td>
                  <form action="/WsWorkunits/WUAction?WUActionType=Pause&amp;Wuids_i1={$wuid}" method="post">
                    <input type="submit" name="Pause" value="Pause" class="sbutton" title="Pause workunit">
                      <xsl:if test="number(AccessFlag) &lt; 7 or State!='running'">
                        <xsl:attribute name="disabled">disabled</xsl:attribute>
                      </xsl:if>
                    </input>
                  </form>
                </td>
                <td>
                  <form action="/WsWorkunits/WUAction?WUActionType=PauseNow&amp;Wuids_i1={$wuid}" method="post">
                    <input type="submit" name="PauseNow" value="PauseNow" class="sbutton" title="Pause workunit now">
                      <xsl:if test="number(AccessFlag) &lt; 7 or State!='running'">
                        <xsl:attribute name="disabled">disabled</xsl:attribute>
                      </xsl:if>
                    </input>
                  </form>
                </td>
                <td>
                  <form action="/WsWorkunits/WUAction?WUActionType=Resume&amp;Wuids_i1={$wuid}" method="post">
                    <input type="submit" name="Resume" value="Resume" class="sbutton" title="Resume workunit">
                      <xsl:if test="number(AccessFlag) &lt; 7 or State!='paused'">
                        <xsl:attribute name="disabled">disabled</xsl:attribute>
                      </xsl:if>
                    </input>
                  </form>
                </td>
            </xsl:if>
            <td>
              <form action="/WsWorkunits/WUAction?PageFrom=WUID" method="post">
                <xsl:variable name="ScheduledAborting">
                  <xsl:choose>
                    <xsl:when test="State='scheduled' and number(Aborting)">1</xsl:when>
                    <xsl:otherwise>0</xsl:otherwise>
                  </xsl:choose>
                </xsl:variable>
                <input type="hidden" name="Wuids_i1" value="{$wuid}"/>
                <input type="submit" name="WUActionType" value="Abort" class="sbutton" title="Abort workunit" onclick="return confirm('Abort workunit?')">
                  <xsl:if test="number(AccessFlag) &lt; 7 or State='aborting' or State='aborted' or State='failed' or State='completed' or $ScheduledAborting!=0">
                    <xsl:attribute name="disabled">disabled</xsl:attribute>
                  </xsl:if>
                </input>
                <input type="submit" name="WUActionType" value="Delete" class="sbutton" title="Delete workunit" onclick="return confirm('Delete workunit?')">
                  <xsl:if test="number(AccessFlag) &lt; 7 or number(Protected)">
                    <xsl:attribute name="disabled">disabled</xsl:attribute>
                  </xsl:if>
                </input>
                <xsl:if test="number(EventSchedule) = 1">
                  <input type="submit" name="WUActionType" value="Reschedule" class="sbutton" title="Reschedule workunit">
                            <xsl:if test="number(AccessFlag) &lt; 7">
                              <xsl:attribute name="disabled">disabled</xsl:attribute>
                            </xsl:if>
                         </input>
                </xsl:if>
                <xsl:if test="number(EventSchedule) = 2">
                  <input type="submit" name="WUActionType" value="Deschedule" class="sbutton" title="Deschedule workunit">
                            <xsl:if test="number(AccessFlag) &lt; 7">
                              <xsl:attribute name="disabled">disabled</xsl:attribute>
                            </xsl:if>
                        </input>
                </xsl:if>
              </form>
            </td>
            <td>
              <form action="/WsWorkunits/WUResubmit?Wuids_i1={$wuid}" method="post">
                <input type="submit" name="Recover" value="Recover" class="sbutton" title="Attempt to resume running workunit from where it stopped">
                  <xsl:if test="number(AccessFlag) &lt; 7 or State!='aborted' and State!='failed'">
                    <xsl:attribute name="disabled">disabled</xsl:attribute>
                  </xsl:if>
                </input>
              </form>
            </td>
            <td>
              <form action="/WsWorkunits/WUResubmit?Wuids_i1={$wuid}&amp;ResetWorkflow=1" method="post">
                <input type="submit" name="Resubmit" value="Resubmit" title="Clean and rerun the workunit" class="sbutton">
                  <xsl:if test="number(AccessFlag) &lt; 7 or State!='aborted' and State!='failed' and State!='completed' and State!='archived'">
                    <xsl:attribute name="disabled">disabled</xsl:attribute>
                  </xsl:if>
                </input>
              </form>
            </td>
            <td>
              <form action="/WsWorkunits/WUResubmit?Wuids_i1={$wuid}&amp;CloneWorkunit=1" method="post">
                <input type="submit" name="CloneWU" value="Clone" title="Submit as new workunit" class="sbutton">
                  <xsl:if test="number(AccessFlag) &lt; 7 or State!='aborted' and State!='failed' and State!='completed' and State!='archived'">
                    <xsl:attribute name="disabled">disabled</xsl:attribute>
                  </xsl:if>
                </input>
              </form>
            </td>
          </tr>
        </table>
      </xsl:otherwise>
    </xsl:choose>
       </div>
       </div>

  </xsl:template>

  <xsl:template match="SourceFiles/ECLSourceFile">
    <tr>
      <td align="left">
      </td>
      <td align="left">
        <xsl:if test="count(ECLSourceFiles/ECLSourceFile)&gt;0">
          <A href="javascript:void(0)" onclick="toggleFileElement('{Name}');" id="explink{Name}" class="wufileexpand">&nbsp;</A>
        </xsl:if>
        <xsl:if test="count(ECLSourceFiles/ECLSourceFile)=0">
          &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
        </xsl:if>
        <a href="/WsDfu/DFUInfo?Name={Name}" >
          <xsl:value-of select="Name"/>
          (<xsl:value-of select="FileCluster"/>)

          <xsl:if test="number(Count)>1">
            (<xsl:value-of select="Count"/> times)
          </xsl:if>
      <xsl:if test="number(IsSuperFile)>0">
        ****
      </xsl:if>
        </a>
      </td>
    </tr>
    <xsl:if test="count(ECLSourceFiles/ECLSourceFile)&gt;0">
      <tr id="{Name}" class="wusectioncontent">
        <td align="left">
        </td>
        <td align="left">
        <table>
          <colgroup>
            <col width="5px"/>
            <col/>
          </colgroup>
          <xsl:apply-templates select="ECLSourceFiles/ECLSourceFile"/>
        </table>
        </td>
      </tr>
    </xsl:if>
  </xsl:template>

  <xsl:template match="ECLSourceFiles/ECLSourceFile">
    <tr>
      <td align="left">
      </td>
      <td align="left">
        <xsl:if test="count(ECLSourceFiles/ECLSourceFile)&gt;0">
          <A href="javascript:void(0)" onclick="toggleFileElement('{Name}');" id="explink{Name}" class="wufileexpand">&nbsp;</A>
        </xsl:if>
        <xsl:if test="count(ECLSourceFiles/ECLSourceFile)=0">
          &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
        </xsl:if>
        <a href="/WsDfu/DFUInfo?Name={Name}" >
          <xsl:value-of select="Name"/>
          (<xsl:value-of select="FileCluster"/>)

          <xsl:if test="number(Count)>1">
            (<xsl:value-of select="Count"/> times)
          </xsl:if>
          <xsl:if test="number(IsSuperFile)>0">
            ****
          </xsl:if>
        </a>
      </td>
    </tr>
    <xsl:if test="count(ECLSourceFiles/ECLSourceFile)&gt;0">
      <tr id="{Name}" class="wusectioncontent">
        <td align="left">
        </td>
        <td align="left">
          <table>
            <colgroup>
              <col width="5px"/>
              <col/>
            </colgroup>
            <xsl:apply-templates select="ECLSourceFiles/ECLSourceFile"/>
          </table>
        </td>
      </tr>
    </xsl:if>
  </xsl:template>

  <xsl:template match="ECLException">
    <xsl:variable name="rowclass" select="position() mod 2" />
    <tr class="{$rowclass}">
      <td>
        <b>
          <xsl:value-of select="Source"/>
        </b>
      </td>
      <td>
        <xsl:variable name="pos">
          <xsl:value-of select="FileName"/>
          <xsl:if test="number(LineNo) and number(Column)">
            (<xsl:value-of select="LineNo"/>,<xsl:value-of select="Column"/>)
          </xsl:if>
        </xsl:variable>

        <xsl:if test="string-length($pos)">
          <xsl:value-of select="$pos"/>:
        </xsl:if>

        <xsl:value-of select="Code"/>:
        <xsl:value-of select="Message" disable-output-escaping="yes"/>
      </td>
    </tr>
  </xsl:template>
  <xsl:template match="ECLResult">
    <xsl:variable name="rowclass" select="position() mod 2" />
    <xsl:variable name="position" select="position()" />
      <tr id="ECL_Result_{position()}" class="{$rowclass}">
      <td>
        <xsl:choose>
          <xsl:when test="string-length(Name)">
            <xsl:if test="(Value = '[undefined]' or Value = '') and contains(Name, 'Result')">
            <xsl:text disable-output-escaping="yes"><![CDATA[<span style="color:silver">]]></xsl:text>
            </xsl:if>
            <xsl:call-template name="id2string">
              <xsl:with-param name="toconvert" select="Name"/>
            </xsl:call-template>
            <xsl:if test="(Value = '[undefined]' or Value = '') and contains(Name, 'Result')">
            <xsl:text disable-output-escaping="yes"><![CDATA[</span>]]></xsl:text>
            </xsl:if>
          </xsl:when>
          <xsl:otherwise>
            <xsl:if test="Value = '[undefined]'">
             <span style="color:silver">Result</span>
            </xsl:if>
            <xsl:if test="Value != '[undefined]'">
             <b>Result</b>
            </xsl:if>
          </xsl:otherwise>
        </xsl:choose>
        <xsl:if test="number(IsSupplied)"> supplied</xsl:if>
      </td>
     <xsl:choose>
       <xsl:when test="((string-length(FileName) &lt; 1) or number(ShowFileContent)) and string-length(Link)">
          <td>
            <a href="javascript:void(0);" onclick="getLink(document.getElementById('ECL_Result_{position()}'), '/WsWorkunits/WUResult?Wuid={$wuid}&amp;Sequence={Link}');return false;">
              <xsl:value-of select="Value"/>
            </a>
          </td>
          <td>
            <a href="javascript:void(0);" onclick="getLink(document.getElementById('ECL_Result_{position()}'), '/WsWorkunits/WUResultBin?Format=zip&amp;Wuid={$wuid}&amp;Sequence={Link}');return false;">.zip</a>
          </td>
          <td>
            <a href="javascript:void(0);" onclick="getLink(document.getElementById('ECL_Result_{position()}'), '/WsWorkunits/WUResultBin?Format=gzip&amp;Wuid={$wuid}&amp;Sequence={Link}');return false;">.gz</a>
          </td>
          <td>
            <a href="javascript:void(0);" onclick="getLink(document.getElementById('ECL_Result_{position()}'), '/WsWorkunits/WUResultBin?Format=xls&amp;Wuid={$wuid}&amp;Sequence={Link}');return false;">.xls</a>
          </td>
          <td>
            <xsl:if test="string-length(FileName)">
              <a href="/WsDfu/DFUInfo?Name={FileName}" >
                <xsl:value-of select="FileName"/>
              </a>
            </xsl:if>
          </td>
          <xsl:variable name="resultname" select="Name"/>
          <xsl:for-each select="/WUInfoResponse/ResultViews/View">
            <td>
              <a href="javascript:void(0);" onclick="getLink(document.getElementById('ECL_Result_{$position}'), '/WsWorkunits/WUResultView?Wuid={$wuid}&amp;ResultName={$resultname}&amp;ViewName={.}');return false;"><xsl:value-of select="."/></a>
            </td>
          </xsl:for-each>
        </xsl:when>
        <xsl:when test="number(ShowFileContent) and string-length(FileName)">
          <td>
            <a href="/WsWorkunits/WUResult?LogicalName={FileName}" >
              <xsl:value-of select="Value"/>
            </a>
          </td>
          <td/>
          <td/>
          <td/>
          <td>
            <xsl:if test="string-length(FileName)">
              <a href="/WsDfu/DFUInfo?Name={FileName}" >
                <xsl:value-of select="FileName"/>
              </a>
            </xsl:if>
          </td>
       </xsl:when>
       <xsl:when test="Value = '[undefined]'">
         <td>
            <xsl:text disable-output-escaping="yes"><![CDATA[ <span style="color:silver">]]></xsl:text>
            <xsl:value-of select="Value"/>
            <xsl:text disable-output-escaping="yes"><![CDATA[ </span>]]></xsl:text>
         </td>
         <td/>
         <td/>
         <td/>
         <td>
            <xsl:if test="string-length(FileName)">
                <a href="/WsDfu/DFUInfo?Name={FileName}" >
                    <xsl:value-of select="FileName"/>
                </a>
            </xsl:if>
         </td>
       </xsl:when>
       <xsl:otherwise>
         <td/>
         <td/>
         <td/>
         <td/>
         <td><xsl:value-of select="Value"/></td>
         <td>
            <xsl:if test="string-length(FileName)">
                <a href="/WsDfu/DFUInfo?Name={FileName}" >
                    <xsl:value-of select="FileName"/>
                </a>
            </xsl:if>
         </td>
       </xsl:otherwise>
     </xsl:choose>
    </tr>
  </xsl:template>
  <xsl:template match="ECLGraph">
    <xsl:variable name="rowclass" select="position() mod 2" />
    <tr class="{$rowclass}">
      <xsl:choose>
        <xsl:when test="number(Running)">
          <xsl:attribute name="class">running</xsl:attribute>
        </xsl:when>
        <xsl:when test="number(Complete)">
        </xsl:when>
        <xsl:when test="number(Failed)">
          <xsl:attribute name="class">red</xsl:attribute>
        </xsl:when>
        <xsl:otherwise>
          <xsl:attribute name="class">grey</xsl:attribute>
        </xsl:otherwise>
      </xsl:choose>
      <td>
        <a href="javascript:void(0)" onclick="return loadUrl('/WsWorkunits/GVCAjaxGraph?Name={$wuid}&amp;GraphName={Name}');" >
          <xsl:value-of select="Name"/>
          <xsl:if test="number(RunningId)">
            (<xsl:value-of select="RunningId"/>)
          </xsl:if>
        </a>
      </td>
      <td>
        <a href="javascript:void(0)" onclick="return loadUrl('/WsWorkunits/GVCAjaxGraph?Name={$wuid}&amp;GraphName={Name}');" >
          <xsl:value-of select="Label"/>
        </a>
      </td>
    </tr>
  </xsl:template>
  <xsl:template match="ECLTimer">
    <xsl:variable name="rowclass" select="position() mod 2" />
    <tr class="{$rowclass}">
      <td>
        <xsl:if test="GraphName and SubGraphId">
          <a href="javascript:void(0)" onclick="return loadUrl('/WsWorkunits/GVCAjaxGraph?Name={$wuid}&amp;GraphName={GraphName}&amp;SubGraphId={SubGraphId}');" >
            <xsl:value-of select="Name"/>
          </a>
        </xsl:if>
        <xsl:if test="GraphName and not(SubGraphId)">
          <a href="javascript:void(0)" onclick="return loadUrl('/WsWorkunits/GVCAjaxGraph?Name={$wuid}&amp;GraphName={GraphName}');" >
            <xsl:value-of select="Name"/>
          </a>
        </xsl:if>
        <xsl:if test="not(GraphName)">
          <xsl:value-of select="Name"/>
        </xsl:if>
      </td>
      <td style="text-align:right;">
        <xsl:if test="GraphName and SubGraphId">
          <a href="javascript:void(0)" onclick="return loadUrl('/WsWorkunits/GVCAjaxGraph?Name={$wuid}&amp;GraphName={GraphName}&amp;SubGraphId={SubGraphId}');" >
            <xsl:value-of select="Value"/>
          </a>
        </xsl:if>
        <xsl:if test="GraphName and not(SubGraphId)">
          <a href="javascript:void(0)" onclick="return loadUrl('/WsWorkunits/GVCAjaxGraph?Name={$wuid}&amp;GraphName={GraphName}');" >
            <xsl:value-of select="Value"/>
          </a>
        </xsl:if>
        <xsl:if test="not(GraphName)">
          <xsl:value-of select="Value"/>
        </xsl:if>
      </td>
      <td>
        <xsl:if test="number(Count)>1">
          (<xsl:value-of select="Count"/> calls)
        </xsl:if>
      </td>
    </tr>
  </xsl:template>
  <xsl:template match="ResourceURLs">
    <xsl:for-each select="URL">
    <tr>
      <td>
        <a href="{text()}" >
          <xsl:value-of select="."/>
        </a>
      </td>
    </tr>
    </xsl:for-each>
  </xsl:template>
  <xsl:template match="ECLHelpFile">
    <tr>
      <xsl:if test="Type = 'cpp'">
        <td>
          <a href="/WsWorkunits/WUFile/{Name}?Wuid={$wuid}&amp;Name={Name}&amp;IPAddress={IPAddress}&amp;Description={Description}&amp;Type=cpp" >
            <xsl:value-of select="Description"/><xsl:if test="number(FileSize)>640000"> (Truncated to 640K bytes)</xsl:if>
          </a>
        </td>
        <td>
          <a href="javascript:void(0)" onclick="getOptions('{Description}', '/WsWorkunits/WUFile/{Name}?Wuid={$wuid}&amp;Name={Name}&amp;IPAddress={IPAddress}&amp;Description={Description}&amp;Type=cpp', false); return false;">
            download
          </a>
        </td>
        <!--td>cpp</td-->
      </xsl:if>
      <xsl:if test="Type = 'dll'">
        <td>
          <xsl:value-of select="Description"/>
        </td>
        <td>
          <a href="javascript:void(0)" onclick="getOptions('{Description}', '/WsWorkunits/WUFile/{$wuid}.dll?Wuid={$wuid}&amp;Name={Name}&amp;Type=dll', false); return false;">
            download
          </a>
        </td>
      </xsl:if>
      <xsl:if test="(Type = 'xml') or (Type = 'hint')">
        <td>
          <a href="/esp/iframe?esp_iframe_title=ECL Workunit - {$wuid} - {Description}&amp;inner=
                   /WsWorkunits/WUFile%3fWuid%3d{$wuid}%26Name%3d{Name}%26IPAddress%3d{IPAddress}%26Description%3d{Description}%26Type%3dXML" >
            <xsl:value-of select="Description"/><xsl:if test="number(FileSize)>640000"> (Truncated to 640K bytes)</xsl:if>
          </a>
        </td>
        <td>
          <a href="javascript:void(0)" onclick="getOptions('{Description}', '/WsWorkunits/WUFile/{Name}?Wuid={$wuid}&amp;Name={Name}&amp;IPAddress={IPAddress}&amp;Description={Description}&amp;Type=xml', false); return false;">
            download
          </a>
        </td>
      </xsl:if>
      <xsl:if test="Type = 'res'">
        <td>
          <a href="/WsWorkunits/WUFile/res.txt?Wuid={$wuid}&amp;Type=res">res.txt</a>
        </td>
        <td>
          <a href="javascript:void(0)" onclick="getOptions('res.txt', '/WsWorkunits/WUFile/res.txt?Wuid={$wuid}&amp;Type=res', false); return false;">download</a>
        </td>
      </xsl:if>
      <xsl:if test="starts-with(Type, 'ThorLog')">
        <td>
          <a href="/WsWorkunits/WUFile/ThorLog?Wuid={$wuid}&amp;Name={Name}&amp;Type={Type}"
                        >
            thormaster.log: <xsl:value-of select="Name"/>
          </a>
        </td>
        <td>
          <a href="javascript:void(0)" onclick="getOptions('thormaster.log', '/WsWorkunits/WUFile/ThorLog?Wuid={$wuid}&amp;Name={Name}&amp;Type={Type}', false); return false;">
            download
          </a>
        </td>
      </xsl:if>
      <xsl:if test="Type = 'EclAgentLog'">
        <td>
          <a href="/WsWorkunits/WUFile/EclAgentLog?Wuid={$wuid}&amp;Name={Name}&amp;Process={PID}&amp;Type=EclAgentLog">
              eclagent.log: <xsl:value-of select="Name"/>
          </a>
        </td>
        <td>
          <a href="javascript:void(0)" onclick="getOptions('eclagent.log', '/WsWorkunits/WUFile/EclAgentLog?Wuid={$wuid}&amp;Name={Name}&amp;Process={PID}&amp;Type=EclAgentLog', false); return false;">
            download
          </a>
        </td>
      </xsl:if>
    </tr>
  </xsl:template>

  <xsl:template match="ECLSourceFile">
    <tr>
      <td>
        <a href="/WsDfu/DFUInfo?Name={Name}" >
          <xsl:if test="number(Count)>1">
            (<xsl:value-of select="Count"/> times)
          </xsl:if>
        </a>
      </td>
    </tr>
  </xsl:template>

  <xsl:template match="WUActionFailures">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
      <head>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="files_/css/sortabletable.css"/>
        <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
          <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
        </script>
        <script language="JavaScript1.2">
          <xsl:text disable-output-escaping="yes"><![CDATA[
                     function onRowCheck(checked)
                     {
                        protectedChecked = 0;
                        unprotectedChecked = 0;
                        document.getElementById("deleteBtn").disabled = true;
                        document.getElementById("protectBtn").disabled = true;
                        document.getElementById("unprotectBtn").disabled = true;

                        checkSelected(document.forms['listitems']);
                        if (protectedChecked > 0 && unprotectedChecked == 0)
                        {
                            document.getElementById("unprotectBtn").disabled = false;
                        }
                        else if (unprotectedChecked > 0 && protectedChecked == 0)
                        {
                            document.getElementById("deleteBtn").disabled = false;
                            document.getElementById("protectBtn").disabled = false;
                        }
                     }                     
                 function launch(link)
                 {
                    document.location.href=link.href;
                 }                     
                     function onLoad()
                     {
                        initSelection('resultsTable');
                     }       
               ]]></xsl:text>
        </script>
      </head>
      <body class="yui-skin-sam" onload="onLoad()">
        <h1>Exception(s) occurred:</h1>
        <table class="sort-table" id="resultsTable">
          <thead>
            <tr>
              <td style="cursor:pointer">
                <strong>WUID</strong>
              </td>
              <td style="cursor:pointer">
                <strong>Action</strong>
              </td>
              <td style="cursor:pointer">
                <strong>Reason</strong>
              </td>
            </tr>
          </thead>
          <tbody>
            <xsl:apply-templates/>
          </tbody>
        </table>
        <br/>
        <input id="backBtn" type="button" value="Go Back" onclick="history.go(-index)" style="display:none"> </input>
      </body>
    </html>
  </xsl:template>

  <xsl:template match="Workflows" mode="list">
    <table class="sort-table" id="resultsTable">
      <colgroup>
        <col width="5"/>
        <col width="120"/>
        <col width="120"/>
        <col width="120"/>
        <col width="120"/>
      </colgroup>
      <thead>
        <tr class="grey">
          <th>WFID</th>
          <th>Event Name</th>
          <th>Event Text</th>
          <th>Count</th>
          <th>Count Remaining</th>
        </tr>
      </thead>
      <tbody>
        <xsl:for-each select="ECLWorkflow">
          <!--xsl:sort select="WFID"/-->
          <tr>
            <td>
              <xsl:value-of select="WFID"/>
            </td>
            <td>
              <xsl:value-of select="EventName"/>
            </td>
            <td>
              <xsl:value-of select="EventText"/>
            </td>
            <td>
              <xsl:if test="number(Count) > -1">
                <xsl:value-of select="Count"/>
              </xsl:if>
            </td>
            <td>
              <xsl:if test="number(CountRemaining) > -1">
                <xsl:value-of select="CountRemaining"/>
              </xsl:if>
            </td>
          </tr>
        </xsl:for-each>
      </tbody>
    </table>
  </xsl:template>

  <xsl:template match="WUGraphTimingResponse">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
      <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <title>
          <xsl:value-of select="$wuid0"/>
        </title>
        <link REL="stylesheet" TYPE="text/css" HREF="/esp/files/default.css"/>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <script type="text/javascript" src="files_/scripts/tooltip.js">
          <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
        </script>
        <link type="text/css" rel="StyleSheet" href="/esp/files_/css/sortabletable.css"/>
        <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
          <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
        </script>
        <script type="text/javascript">
          var wuid = '<xsl:value-of select="$wuid0"/>';
          <xsl:text disable-output-escaping="yes"><![CDATA[
                  
                     function onLoad()
                     {
                        initSelection('resultsTable');
                     }               
                       
                     function ChangeHeader(o1, headerid)
                     {
                        if (headerid%2)
                        {
                            o1.bgColor = '#CCCCCC';
                        }
                        else
                        {
                            o1.bgColor = '#F0F0FF';
                        }
                     }

                     function launch(link)
                     {
                        document.location.href=link.href;
                     }     

                     function selectSubGraph(GraphName, SubGraphId)
                     {
                        if (window.opener)
              {
                              window.opener.selectGraphSubGraph(GraphName.substring(5), SubGraphId);
              }
              else
              {
                // Load the Graph page directly.
                var urlBase = '/WsWorkunits/GVCAjaxGraph?Name=' + wuid + '&GraphName=' + GraphName + '&SubGraphId=' + SubGraphId;
                document.location.href = urlBase;
              }
                     } 
   
               ]]></xsl:text>
        </script>
      </head>
      <body class="yui-skin-sam" onload="onLoad()">
        <h3>
          Graph Timings for :
          <a href="/WsWorkunits/WUInfo?Wuid={$wuid0}" >
            <xsl:value-of select="$wuid0"/>
          </a>
        </h3>
        <table class="sort-table" id="resultsTable">
          <colgroup>
            <col/>
            <col class="number"/>
            <col class="number"/>
            <col class="number"/>
            <col class="number"/>
          </colgroup>
          <thead>
            <tr>
              <th align="center" style="cursor:pointer" onmouseover="ChangeHeader(this, 0)" onmouseout="ChangeHeader(this, 1)">ID</th>
              <th align="center" style="cursor:pointer" onmouseover="ChangeHeader(this, 0)" onmouseout="ChangeHeader(this, 1)">Graph #</th>
              <th align="center">Sub Graph #</th>
              <th align="center" style="cursor:pointer" onmouseover="ChangeHeader(this, 0)" onmouseout="ChangeHeader(this, 1)">Minutes</th>
              <th align="center" style="cursor:pointer" onmouseover="ChangeHeader(this, 0)" onmouseout="ChangeHeader(this, 1)">Milliseconds</th>
            </tr>
          </thead>
          <tbody>
            <xsl:apply-templates select="Workunit/TimingData/ECLTimingData"/>
          </tbody>
        </table>
      </body>
    </html>
  </xsl:template>

  <xsl:template match="Workunit/TimingData/ECLTimingData">
    <tr>
      <td align="left">
        <xsl:text disable-output-escaping="yes"><![CDATA[<a href="javascript:void(0)" onclick="selectSubGraph('graph]]></xsl:text>
        <xsl:value-of select="GraphNum"/>
        <xsl:text disable-output-escaping="yes"><![CDATA[', ]]></xsl:text>
        <xsl:value-of select="GID"/>
        <xsl:text disable-output-escaping="yes"><![CDATA[);return false;">]]></xsl:text>
        <xsl:value-of select="GID"/>
        <xsl:text disable-output-escaping="yes"><![CDATA[ </a> ]]></xsl:text>
      </td>
      <td align="left">
        <xsl:value-of select="GraphNum"/>
      </td>
      <td align="left">
        <xsl:value-of select="SubGraphNum"/>
      </td>
      <td align="right">
        <xsl:value-of select="Min"/>
      </td>
      <td align="right">
        <xsl:value-of select="MS"/>
      </td>
    </tr>
  </xsl:template>

  <xsl:template match="text()|comment()"/>

  <xsl:template name="timeformat">
    <xsl:param name="toformat"/>
    <xsl:if test="string-length($toformat) > 0">
      <xsl:variable name="milli" select="substring-after($toformat, '.')"/>
      <xsl:if test="contains($toformat, ':')">
      <xsl:call-template name="timezerofill">
        <xsl:with-param name="tofill" select="substring-before($toformat, '.')"/>
      </xsl:call-template>
      </xsl:if>
      <xsl:value-of select="$milli"/>
    </xsl:if>
  </xsl:template>

  <xsl:template name="timezerofill">
    <xsl:param name="tofill"/>
    <xsl:if test="string-length($tofill) > 0">
      <xsl:variable name="timeval" select="substring-after($tofill, ':')"/>
      <xsl:if test="string-length($timeval)=1">
      0
      </xsl:if>
      <xsl:value-of select="$timeval"/>.
    </xsl:if>
  </xsl:template>

</xsl:stylesheet>
