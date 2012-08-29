/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */

Usage:
   soapplus [action/options]

Actions: select one
   -h/-? : show this usage page
   -c : start in client mode. This is the default action.
   -s : start in server mode.
   -x : start in proxy mode.
   -b : start in hybrid mode as both a client and a server.
   -g : automatically generate the requests from wsdl/xsd. url?wsdl is used when none is specified
   -t : test schema parser
   -diff <left> <right> : compare 2 files or directories. You can ignore certain xpaths, as specified in the config file (with -cfg option). An example of config file is: <whatever><Ignore><XPath>/soap:Body/AAC_AuthorizeRequest/ClientIP</XPath><XPath>/soap:Header/Security/UsernameToken/Username</XPath></Ignore></whatever>
   -stress <threads> <seconds> : run stress test, with the specified number of threads, for the specified number of seconds.

Options: 
   -url <[http|https]://[user:passwd@]host:port/path>: the url to send requests to. For esp, the path is normally the service name, not the method name. For examle, it could be WsADL, WsAccurint, WsIdentity etc.
   -i <file or directory name> : input file/directory containing the requests (for client mode) or responses(for server mode). It can also contain wildcards, for example .\inputs\*.xml. If not specified, a predefined GET will be performed on the url.
   -si <file or directory name> : input file/directory containing the response(s) for servermode or hybrid mode. 
        If not specified, a predefined simple response will be used for every request.
        If a single file is specified, the file will be used for every request
        If a directory is specified, the file that has the same file name as the request will be used.
   -o <directory-name> : directory to put the outputs.
   -w : create output directories and write the output files. When -o is not specified,  a 'SOAPPLUS.current-date-time' directory under current directory will be used.
   -p : port to listen on.
   -d <trace-level> : 0 no tracing, >=1 a little tracing, >=5 some tracing, >=10 all tracing. If -w is specified, default tracelevel is 5, if -stress is specified, it's 1, otherwise it's 10.
   -v : validate the response. If -xsd is not specified, url?xsd will be used to retrieve the xsd.
   -xsd <path-or-url> : the path or url to the xsd file to be used for validation.
   -wsdl <path-or-url> : the path or url to the wsdl file to be used to generate requests.
   -r : the xsd/or wsdl is in roxie response format.
   -ra : generate all datasets in schema (default: only these exists in template).
   -n : generate n elements for array type [default: 1]
   -m <method-name> : the method to generate the request for. If not specified, a request will be generated for every method in the wsdl.
   -a : for server or hybrid mode, tell the server to abort before finish sending back response.
   -cfg <file-name> : configuration files for soapplus
   -gx : use *** MISSING *** as the default value for missing string fields
   -gs : don't generate soap message wrap
   -gf : the output filename for generated message
   -delay <min-milli-seconds> <max-milli-seconds> : randomly delay between requests in stress test. By default requests are sent without stop.
   -ooo : the 2 input files to diff are Out-Of-Order, so do the best-match calculation while comparing them. (this will slow it down dramatically for big xml files).
   -y: use default answers to yes or no prompts.
