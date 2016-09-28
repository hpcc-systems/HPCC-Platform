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
############################################################################## */
#if !defined(AFX_XMLTAGS_H__57887617_2BDE_4E3E_92CB_43FCDEDD05B8__INCLUDED_)
#define AFX_XMLTAGS_H__57887617_2BDE_4E3E_92CB_43FCDEDD05B8__INCLUDED_
//---------------------------------------------------------------------------

#define XML_HEADER                     "?xml version=\"1.0\" encoding=\"UTF-8\"?"
#define XML_TAG_INCLUDE                "Include"

#define XML_TAG_ENVIRONMENT            "Environment"
#define XML_TAG_HARDWARE               "Hardware"
#define XML_TAG_SOFTWARE               "Software"
#define XML_TAG_PROGRAMS               "Programs"
#define XML_TAG_DATA                   "Data"
#define XML_TAG_ENVSETTINGS            "EnvSettings"
#define XML_TAG_DIRECTORIES            "Directories"

#define XML_TAG_ATTRIBUTESERVER        "AttributeServer"
#define XML_TAG_ATTRSERVERINSTANCE     "AttrServerInstance"
#define XML_TAG_AUTHENTICATION         "Authentication"
#define XML_TAG_BUILD                  "Build"
#define XML_TAG_BUILDSET               "BuildSet"
#define XML_TAG_CLUSTER                "Cluster"
#define XML_TAG_COMPONENT              "Component"
#define XML_TAG_COMPONENTS             "Components"
#define XML_TAG_COMPUTER               "Computer"
#define XML_TAG_COMPUTERTYPE           "ComputerType"
#define XML_TAG_DAFILESERVERPROCESS    "DafilesrvProcess"
#define XML_TAG_DALISERVERINSTANCE     "DaliServerInstance"
#define XML_TAG_DALISERVERPROCESS      "DaliServerProcess"
#define XML_TAG_DOMAIN                 "Domain"
#define XML_TAG_DROPZONE               "DropZone"
#define XML_TAG_ECLAGENTPROCESS        "EclAgentProcess"
#define XML_TAG_ECLBATCHPROCESS        "EclBatchProcess"
#define XML_TAG_ECLSERVERPROCESS       "EclServerProcess"
#define XML_TAG_ECLCCSERVERPROCESS     "EclCCServerProcess"
#define XML_TAG_ECLSCHEDULERPROCESS    "EclSchedulerProcess"
#define XML_TAG_ESPBINDING             "EspBinding"
#define XML_TAG_ESPPROCESS             "EspProcess"
#define XML_TAG_ESPSERVICE             "EspService"
#define XML_TAG_FILE                   "File"
#define XML_TAG_FUNCTION               "Function"
#define XML_TAG_INSTALLSET             "InstallSet"
#define XML_TAG_INSTANCE               "Instance"
#define XML_TAG_INSTANCES              "Instances"
#define XML_TAG_LDAPSERVERPROCESS      "LDAPServerProcess"
#define XML_TAG_LOCATION               "Location"
#define XML_TAG_MAP                    "Map"
#define XML_TAG_MAPTABLES              "MapTables"
#define XML_TAG_MODEL                  "Model"
#define XML_TAG_MODULE                 "Module"
#define XML_TAG_NAS                    "NAS"
#define XML_TAG_NODE                   "Node"
#define XML_TAG_NODES                  "Nodes"
#define XML_TAG_NODEREF                "NodeRef"
#define XML_TAG_PLUGINTYPE             "PluginType"
#define XML_TAG_PLUGINPROCESS          "PluginProcess"
#define XML_TAG_PLUGINREF              "PluginRef"
#define XML_TAG_ROXIE_FARM             "RoxieFarmProcess"
#define XML_TAG_ROXIE_SERVER           "RoxieServerProcess"
#define XML_TAG_ROXIE_SERVER_PROCESSS  XML_TAG_ROXIE_SERVER
#define XML_TAG_ROXIE_SLAVE            "RoxieSlaveProcess"
#define XML_TAG_ROXIE_ONLY_SLAVE       "RoxieSlave"
#define XML_TAG_ROXIE_CHANNEL          "RoxieChannel"
#define XML_TAG_RUNECLPROCESS          "RunEclProcess"
#define XML_TAG_SASHA_SERVER_PROCESS   "SashaServerProcess"
#define XML_TAG_SPAREPROCESS           "SpareProcess"
#define XML_TAG_SYBASEPROCESS          "SybaseProcess"
#define XML_TAG_SWITCH                 "Switch"
#define XML_TAG_THORCLUSTER            "ThorCluster"
#define XML_TAG_THORMASTERPROCESS      "ThorMasterProcess"
#define XML_TAG_THORSLAVEPROCESS       "ThorSlaveProcess"
#define XML_TAG_THORSPAREPROCESS       "ThorSpareProcess"
#define XML_TAG_THORREF                "ThorRef"
#define XML_TAG_TOPOLOGY               "Topology"
#define XML_TAG_VERSION                "Version"
#define XML_TAG_ROXIECLUSTER           "RoxieCluster"
#define XML_TAG_LOCALENVFILE           "LocalEnvFile"
#define XML_TAG_LOCALCONFFILE          "LocalConfFile"
#define XML_TAG_LOCALENVCONFFILE       "LocalEnvConfFile"

#define XML_ATTR_ALIAS                 "@alias"
#define XML_ATTR_AGENTPORT             "@agentPort"
#define XML_ATTR_ATTRIB                "@attrib"
#define XML_ATTR_ATTRSERVER            "@attrServer"
#define XML_ATTR_BACKUPCOMPUTER        "@backupComputer"
#define XML_ATTR_BLOBDIRECTORY         "@blobDirectory"
#define XML_ATTR_BLOBNAME              "@blobName"
#define XML_ATTR_BREAKOUTLIMIT         "@breakoutLimit"
#define XML_ATTR_BUILD                 "@build"
#define XML_ATTR_BUILDSET              "@buildSet"
#define XML_ATTR_CHANNEL               "@channel"
#define XML_ATTR_CLUSTER               "@cluster"
#define XML_ATTR_CLUSTERNAME           "@clusterName"
#define XML_ATTR_CHECKCANCELLED        "@checkCancelled"
#define XML_ATTR_COMPILEOPTIONS        "@compileOptions"
#define XML_ATTR_COMPILERPATH          "@compilerPath"
#define XML_ATTR_COMPRESSC             "@compressC"
#define XML_ATTR_COMPRESSD             "@compressD"
#define XML_ATTR_COMPTYPE              "@compType"
#define XML_ATTR_COMPUTER              "@computer"
#define XML_ATTR_COMPUTERTYPE          "@computerType"
#define XML_ATTR_COPYDIR               "@copyDir"
#define XML_ATTR_COPYDLL               "@copyDLL"
#define XML_ATTR_CREATEMODELFILE       "@createModelFile"
#define XML_ATTR_CREATESUBDIRECTORIES  "@createSubDirectories"
#define XML_ATTR_CUSTOMERID            "@customerId"
#define XML_ATTR_CUSTOMERNAME          "@customerName"
#define XML_ATTR_DALISERVER            "@daliServer"
#define XML_ATTR_DATABUILD             "@dataBuild"
#define XML_ATTR_LEVEL                 "@level"
#define XML_ATTR_DATAMODEL             "@dataModel"
#define XML_ATTR_DATASEGMENT           "@dataSegment"
#define XML_ATTR_DEBUGXML              "@debugXml"
#define XML_ATTR_DEFAULTHOLE           "@defaultHole"
#define XML_ATTR_DESCRIPTION           "@description"
#define XML_ATTR_DESTPATH              "@destPath"
#define XML_ATTR_DIATHOLEPREFIX        "@diatHolePrefix"
#define XML_ATTR_DIRECTORY             "@directory"
#define XML_ATTR_DLLSTOSLAVES          "@dllsToSlaves"
#define XML_ATTR_DOMAIN                "@domain"
#define XML_ATTR_DRILLDOWN             "@drillDown"
#define XML_ATTR_ECLSERVER             "@eclServer"
#define XML_ATTR_ENDPOINT              "@endpoint"
#define XML_ATTR_EXTERNALPROGDIR       "@externalProgDir"
#define XML_ATTR_FARM                  "@farm"
#define XML_ATTR_FAMILY                "@family"
#define XML_ATTR_FILENAME              "@filename"
#define XML_ATTR_FILESBASEDN           "@filesBasedn"
#define XML_ATTR_FOLDSQL               "@foldSQL"
#define XML_ATTR_FORCECOMPARECLUSTER   "@forceCompareCluster"
#define XML_ATTR_FORCETHORCOMPARECLUSTER "@forceThorCompareCluster"
#define XML_ATTR_FULLXMLLOGGING        "@fullXmlLogging"
#define XML_ATTR_HMON                  "@hmon"
#define XML_ATTR_HOLEDIRECTPREFIX      "@holeDirectPrefix"
#define XML_ATTR_HOLEPREFIX            "@holePrefix"
#define XML_ATTR_HOLEPRIORITY          "@holePriority"
#define XML_ATTR_HOLETIMEOUT           "@holeTimeout"
#define XML_ATTR_HWXPATH               "@hwxpath"
#define XML_ATTR_INCLUDE               "@include"
#define XML_ATTR_INSTALLSET            "@installSet"
#define XML_ATTR_JACKAGENTIP           "@jackAgentIP"
#define XML_ATTR_KEEPCPP               "@keepCPP"
#define XML_ATTR_LABEL                 "@label"
#define XML_ATTR_LDAPSERVER            "@ldapServer"
#define XML_ATTR_LOADERNAME            "@loaderName"
#define XML_ATTR_LOADORDER             "@loadOrder"
#define XML_ATTR_LOCALCACHE            "@localCache"
#define XML_ATTR_LOGDIR                "@logDir"
#define XML_ATTR_LOGICAL_NAME          "@logicalName"
#define XML_ATTR_LOGSQL                "@logSQL"
#define XML_ATTR_MANUFACTURER          "@manufacturer"
#define XML_ATTR_MASK                  "@mask"
#define XML_ATTR_MASTERPORT            "@masterport"
#define XML_ATTR_MAXOCCURS             "@maxOccurs"
#define XML_ATTR_MEMORY                "@memory"
#define XML_ATTR_MINIMALLOGGING        "@minimalLogging"
#define XML_ATTR_MODEL                 "@model"
#define XML_ATTR_MONITORDISKTHRESHOLD  "@monitorDiskThreshold"
#define XML_ATTR_MONITOREMAIL          "@monitorEmail"
#define XML_ATTR_MONITORENABLED        "@monitorEnabled"
#define XML_ATTR_MONITORTIMEOUT        "@monitorTimeout"
#define XML_ATTR_MULTISLAVES           "@multiSlaves"
#define XML_ATTR_MYSQL                 "@mysql"
#define XML_ATTR_NAME                  "@name"
#define XML_ATTR_TARGET                "@target"
#define XML_ATTR_NAMESERVICES          "@nameServices"
#define XML_ATTR_NETADDRESS            "@netAddress"
#define XML_ATTR_NICSPEED              "@nicSpeed"
#define XML_ATTR_NODE                  "@node"
#define XML_ATTR_NOPATCH               "@noPatch"
#define XML_ATTR_NOXMLBLOB             "@noXmlBlob"
#define XML_ATTR_OPSYS                 "@opSys"
#define XML_ATTR_OPTIMIZESQL           "@optimizeSQL"
#define XML_ATTR_ORPHANSASERROR        "@orphansAsError"
#define XML_ATTR_PARAMS                "@params"
#define XML_ATTR_PASSWORD              "@password"
#define XML_ATTR_PATH                  "@path"
#define XML_ATTR_PERFDATALEVEL         "@perfDataLevel"
#define XML_ATTR_PLUGIN                "@plugin"
#define XML_ATTR_PLUGINSPATH           "@pluginsPath"
#define XML_ATTR_PORT                  "@port"
#define XML_ATTR_PREFIX                "@prefix"
#define XML_ATTR_PROCESS               "@process"
#define XML_ATTR_PROCESSCOUNT          "@processCount"
#define XML_ATTR_PROCESS_NAME          "@processName"
#define XML_ATTR_PROTOCOL              "@protocol"
#define XML_ATTR_PROTOTYPE             "@prototype"
#define XML_ATTR_QUEUE                 "@queue"
#define XML_ATTR_REFRESHRATE           "@refreshRate"
#define XML_ATTR_REMOTEFORMAT          "@remoteFormat"
#define XML_ATTR_RETURNTYPE            "@returnType"
#define XML_ATTR_RUNAGENTNAME          "@runAgentName"
#define XML_ATTR_SCHEMA                "@schema"
#define XML_ATTR_SERVERCURDIR          "@serverCurDir"
#define XML_ATTR_SERVICE               "@service"
#define XML_ATTR_SLAVESNAME            "@slavesName"
#define XML_ATTR_SPARESNAME            "@sparesName"
#define XML_ATTR_SRCPATH               "@srcPath"
#define XML_ATTR_SOCACHEDIR            "@socacheDir"
#define XML_ATTR_STATE                 "@state"
#define XML_ATTR_STATUSMSGSENABLED     "@statusMsgsEnabled"
#define XML_ATTR_SUBNET                "@subnet"
#define XML_ATTR_SUFFIX                "@suffix"
#define XML_ATTR_SWITCH                "@switch"
#define XML_ATTR_SYBASE                "@sybase"
#define XML_ATTR_SYNCSENDDLLS          "@syncSendDlls"
#define XML_ATTR_TABLE                 "@table"
#define XML_ATTR_TEMPDIR               "@tempDir"
#define XML_ATTR_THORCONNECTTIMEOUTSECONDS "@thorConnectTimeoutSeconds"
#define XML_ATTR_THORPOLLDELAY         "@thorPollDelay"
#define XML_ATTR_TRACE                 "@trace"
#define XML_ATTR_TRACELEVEL            "@traceLevel"
#define XML_ATTR_TYPE                  "@type"
#define XML_ATTR_URL                   "@url"
#define XML_ATTR_USEBLOBS              "@useBlobs"
#define XML_ATTR_USERNAME              "@username"
#define XML_ATTR_VERSION               "@version"
#define XML_ATTR_WATCHDOGENABLED       "@watchdogEnabled"
#define XML_ATTR_WATCHDOGPROGRESSENABLED  "@watchdogProgressEnabled"
#define XML_ATTR_WATCHDOGPROGRESSINTERVAL "@watchdogProgressInterval"
#define NAME_SPACE_FOR_XSD_SCHEMA      "xs:"

static const char* XSD_TAG_ANNOTATION(NAME_SPACE_FOR_XSD_SCHEMA "annotation");
static const char* XSD_TAG_APP_INFO(NAME_SPACE_FOR_XSD_SCHEMA "appinfo");
static const char* XSD_TAG_ATTRIBUTE(NAME_SPACE_FOR_XSD_SCHEMA "attribute");
static const char* XSD_TAG_ATTRIBUTE_GROUP(NAME_SPACE_FOR_XSD_SCHEMA "attributeGroup");
static const char* XSD_TAG_CHOICE(NAME_SPACE_FOR_XSD_SCHEMA "choice");
static const char* XSD_TAG_COMPLEX_CONTENT(NAME_SPACE_FOR_XSD_SCHEMA "complexContent");
static const char* XSD_TAG_COMPLEX_TYPE(NAME_SPACE_FOR_XSD_SCHEMA "complexType");
static const char* XSD_TAG_DOCUMENTATION(NAME_SPACE_FOR_XSD_SCHEMA "documentation");
static const char* XSD_TAG_ELEMENT(NAME_SPACE_FOR_XSD_SCHEMA "element");
static const char* XSD_TAG_EXTENSION(NAME_SPACE_FOR_XSD_SCHEMA "extension");
static const char* XSD_TAG_KEY(NAME_SPACE_FOR_XSD_SCHEMA "key");
static const char* XSD_TAG_KEYREF(NAME_SPACE_FOR_XSD_SCHEMA "keyref");
static const char* XSD_TAG_SELECTOR(NAME_SPACE_FOR_XSD_SCHEMA "selector");
static const char* XSD_TAG_FIELD(NAME_SPACE_FOR_XSD_SCHEMA "field");
static const char* XSD_TAG_INCLUDE(NAME_SPACE_FOR_XSD_SCHEMA "include");
static const char* XSD_TAG_RESTRICTION(NAME_SPACE_FOR_XSD_SCHEMA "restriction");
static const char* XSD_TAG_SCHEMA("");
static const char* XSD_TAG_SIMPLE_CONTENT(NAME_SPACE_FOR_XSD_SCHEMA "simpleContent");
static const char* XSD_TAG_SEQUENCE(NAME_SPACE_FOR_XSD_SCHEMA "sequence");
static const char* XSD_TAG_SIMPLE_TYPE(NAME_SPACE_FOR_XSD_SCHEMA "simpleType");
static const char* XSD_TAG_ENUMERATION(NAME_SPACE_FOR_XSD_SCHEMA "enumeration");
static const char* XSD_TAG_FRACTION_DIGITS(NAME_SPACE_FOR_XSD_SCHEMA "fractionDigits");
static const char* XSD_TAG_LENGTH(NAME_SPACE_FOR_XSD_SCHEMA "length");
static const char* XSD_TAG_MAX_EXCLUSIVE(NAME_SPACE_FOR_XSD_SCHEMA "maxExclusive");
static const char* XSD_TAG_MAX_INCLUSIVE(NAME_SPACE_FOR_XSD_SCHEMA "maxInclusive");
static const char* XSD_TAG_MIN_EXCLUSIVE(NAME_SPACE_FOR_XSD_SCHEMA "minExlusive");
static const char* XSD_TAG_MIN_INCLUSIVE(NAME_SPACE_FOR_XSD_SCHEMA "minExclusive");
static const char* XSD_TAG_MAX_LENGTH(NAME_SPACE_FOR_XSD_SCHEMA "maxLength");
static const char* XSD_TAG_MIN_LENGTH(NAME_SPACE_FOR_XSD_SCHEMA "minLength");
static const char* XSD_TAG_PATTERN(NAME_SPACE_FOR_XSD_SCHEMA "pattern");
static const char* XSD_TAG_TOTAL_DIGITS(NAME_SPACE_FOR_XSD_SCHEMA "totalDigits");
static const char* XSD_TAG_UNQIUE(NAME_SPACE_FOR_XSD_SCHEMA "unique");
static const char* XSD_TAG_WHITE_SPACE(NAME_SPACE_FOR_XSD_SCHEMA "whiteSpace");

static const char* XSD_DATA_TYPE_NORMALIZED_STRING(NAME_SPACE_FOR_XSD_SCHEMA "normalizedString");
static const char* XSD_DATA_TYPE_STRING(NAME_SPACE_FOR_XSD_SCHEMA "string");
static const char* XSD_DATA_TYPE_TOKEN(NAME_SPACE_FOR_XSD_SCHEMA "token");
static const char* XSD_DATA_TYPE_DATE(NAME_SPACE_FOR_XSD_SCHEMA "date");
static const char* XSD_DATA_TYPE_TIME(NAME_SPACE_FOR_XSD_SCHEMA "time");
static const char* XSD_DATA_TYPE_DATE_TIME(NAME_SPACE_FOR_XSD_SCHEMA "dateTime");
static const char* XSD_DATA_TYPE_DECIMAL(NAME_SPACE_FOR_XSD_SCHEMA "decimal"); // A decimal value
static const char* XSD_DATA_TYPE_INTEGER(NAME_SPACE_FOR_XSD_SCHEMA "integer"); // An integer value
static const char* XSD_DATA_TYPE_INT(NAME_SPACE_FOR_XSD_SCHEMA "int"); // A signed 32-bit integer
static const char* XSD_DATA_TYPE_LONG(NAME_SPACE_FOR_XSD_SCHEMA "long"); // A signed 64-bit integer
static const char* XSD_DATA_TYPE_NON_NEGATIVE_INTEGER(NAME_SPACE_FOR_XSD_SCHEMA "nonNegativeInteger");
static const char* XSD_DATA_TYPE_NON_POSITIVE_INTEGER(NAME_SPACE_FOR_XSD_SCHEMA "nonPositiveInteger");
static const char* XSD_DATA_TYPE_NEGATIVE_INTEGER(NAME_SPACE_FOR_XSD_SCHEMA "negativeInteger");
static const char* XSD_DATA_TYPE_POSITIVE_INTEGER(NAME_SPACE_FOR_XSD_SCHEMA "positiveInteger");
static const char* XSD_DATA_TYPE_BOOLEAN(NAME_SPACE_FOR_XSD_SCHEMA "boolean");

#define UI_FIELD_ATTR_NAME              "@Name"
#define UI_FIELD_ATTR_VALUE             "@Value"
#define UI_FIELD_ATTR_DEFAULTVALUE      "@DefaultValue"

#define TAG_ALIAS                       "alias"
#define TAG_SUBNET                      "subnet"
#define TAG_MASK                        "mask"
#define TAG_TRACE                       "trace"
#define TAG_NAME                        "name"
#define TAG_MANUFACTURER                "manufacturer"
#define TAG_COMPUTERTYPE                "computerType"
#define TAG_OPSYS                       "opSys"
#define TAG_MEMORY                      "memory"
#define TAG_NICSPEED                    "nicSpeed"
#define TAG_USERNAME                    "username"
#define TAG_PASSWORD                    "password"
#define TAG_SNMPSECSTRING               "snmpSecurityString"
#define TAG_NETADDRESS                  "netAddress"
#define TAG_DOMAIN                      "domain"
#define TAG_PROCESS                     "process"
#define TAG_LEVEL                       "level"
#define TAG_COMPUTER                    "computer"
#define TAG_LISTENQUEUE                 "listenQueue"
#define TAG_NUMTHREADS                  "numThreads"
#define TAG_PORT                        "port"
#define TAG_REQARRAYTHREADS             "requestArrayThreads"
#define TAG_ITEMTYPE                    "itemType"
#define TAG_NUMBER                      "number"
#define TAG_METHOD                      "method"
#define TAG_SRCPATH                     "srcPath"
#define TAG_DESTPATH                    "destPath"
#define TAG_DESTNAME                    "destName"
#define TAG_LOCK                        "lock"
#define TAG_CONFIGS                     "configs"
#define TAG_PATH                        "path"
#define TAG_RUNTIME                     "runtime"
#define TAG_BUILD                       "build"
#define TAG_BUILDSET                    "buildset"
#define TAG_DIRECTORY                   "directory"
#define TAG_NODENAME                    "nodeName"
#define TAG_PREFIX                      "prefix"
#define TAG_URL                         "url"
#define TAG_INSTALLSET                  "installSet"
#define TAG_PROCESSNAME                 "processName"
#define TAG_SCHEMA                      "schema"
#define TAG_DEPLOYABLE                  "deployable"
#define TAG_VALUE                       "value"
#define TAG_ATTRIBUTE                   "Attribute"
#define TAG_ELEMENT                     "Element"
#define TAG_DALISERVER                  "daliServer"
#define TAG_LDAPSERVER                  "ldapServer"
#define TAG_SERVICE                     "service"
#define TAG_FILESBASEDN                 "filesBasedn"

//---------------------------------------------------------------------------
#endif // !defined(AFX_XMLTAGS_H__57887617_2BDE_4E3E_92CB_43FCDEDD05B8__INCLUDED_)
