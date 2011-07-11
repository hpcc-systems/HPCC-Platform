@echo off
rem /*##############################################################################
rem 
rem     Copyright (C) 2011 HPCC Systems.
rem 
rem     All rights reserved. This program is free software: you can redistribute it and/or modify
rem     it under the terms of the GNU Affero General Public License as
rem     published by the Free Software Foundation, either version 3 of the
rem     License, or (at your option) any later version.
rem 
rem     This program is distributed in the hope that it will be useful,
rem     but WITHOUT ANY WARRANTY; without even the implied warranty of
rem     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
rem     GNU Affero General Public License for more details.
rem 
rem     You should have received a copy of the GNU Affero General Public License
rem     along with this program.  If not, see <http://www.gnu.org/licenses/>.
rem ############################################################################## */

setlocal
rem set the following parameters to suppress their prompts 
rem set url=
rem set userid=
rem set password=
rem set module=
rem set query=
rem set method=

if "%url%" == "" set /p url=Enter URL for WsECL (as http[s]://^<host^>[:^<port^>]):
if "%userid%" == "" set /p userid=Enter UserId:
if "%password%" == "" set /p password=Enter Password (Warning: Password is echoed back!):
if "%module%" == "" set /p module=Enter ECL Module Name:
if "%query%" == "" set /p query=Enter ECL Query Name:

if "%method%" == "" set /p method=Enter ESP Method Name [%query%]:
if "%method%" == "" set method=%query%

rem you can raise the following tracing level upto 10 (see soapplus -h for more info)
set tracing=0

rem make url2 like http://userid:password@ip_port/module/module.query
set ip_port=%url:*//=%
set fifth_char=%url:~4,1%
set protocol=http
if not "%fifth_char%" == ":" set protocol=%protocol%s
set url2=%protocol%://%userid%:%password%@%ip_port%/%module%/%module%.%query%

echo Processing %method% ...
rem change directory to that of this batch file
for %%D in (%0) do cd /d %%~dpD
set path=%path%;..\..\..\..\bin\debug
md %method% 2> nul
echo Fetching XML schema for the request and generating a request instance %method%Request.xml ...
soapplus -xsd %url2%?xsd -m %module%.%query%Request -d %tracing% -g -gs -wiz -gf %method%\%method%Request.xml

echo Running a dummy request to get a sample response with embedded XML schema in %method%ResponseFull.xml ...
soapplus -url %url2%?format_=xml -d %tracing% -w -o _tmp

move /Y _tmp\response_content\get.txt %method%\%method%ResponseFull.xml
rd /s /q _tmp

rem print the available Datasets in the response
echo The following Datasets are returned in the query response.
grep "Dataset name=" %method%\%method%ResponseFull.xml | perl -ne "if(m/<Dataset name='(.*)'>/){print \"$1\n\"}"

rem the following defines the name of the dataset in the ECL response with results
rem Note that most queries use the name 'Results'
if "%dataset%" == "" set /p dataset=Enter Dataset name in ECL Response [Results]:
if "%dataset%" == "" set dataset=Results

echo Filling in missing XML elements in %method%ResponseFull.xml using embedded XML schema and generating %method%Response.xml ...
soapplus -r -ra -g -xsd %method%\%method%ResponseFull.xml -i %method%\%method%ResponseFull.xml -m %method%Response -wiz -d %tracing% -gf %method%\%method%Response.xml

rem generate CustomNames.xml from CustomNames.txt
..\..\..\..\tools\misc\awk -f ini2xml.awk CustomNames.txt > CustomNames.xml

set xslfile=xml2scm.xsl
set infile=%method%\%method%Request.xml
set outfile=%method%\%method%Request.scm
echo Generating request  interface as %method%Request.scm ...
..\..\..\..\system\xalan\xalan -p method '%method%' -p response 0 -o %outfile% %infile% %xslfile%

set infile=%method%\%method%Response.xml
set outfile=%method%\%method%Response.scm
echo Generating response interface as %method%Response.scm (may take a few minutes) ...
..\..\..\..\system\xalan\xalan -p method '%method%' -p response 1 -p responseDataset '"%dataset%"' -o %outfile% %infile% %xslfile%

set xslfile=xml2qxslt.xsl
set infile=%method%\%method%Request.xml
set outfile=%method%\%method%Request.cpp
echo Generating C++ code for request  as %method%Request.cpp ...
..\..\..\..\system\xalan\xalan -p method '%method%' -p response 0 -o %outfile% %infile% %xslfile%

set infile=%method%\%method%Response.xml
set outfile=%method%\%method%Response.cpp
echo Generating C++ code for response as %method%Response.cpp (may take a few minutes) ...
..\..\..\..\system\xalan\xalan -p method '%method%' -p response 1 -p responseDataset '"%dataset%"'  -o %outfile% %infile% %xslfile%


cd %method%
echo Merging %method%Request.scm and %method%Response.scm into wsm_%method%.scm
copy /Y %method%Request.scm wsm_%method%.scm > nul
type %method%Response.scm >> wsm_%method%.scm
del %method%Re*.scm 2> nul

echo Merging %method%Request.cpp and %method%Response.cpp into rx_%method%.cpp
copy /Y %method%Request.cpp rx_%method%.cpp > nul
type %method%Response.cpp >> rx_%method%.cpp
del %method%Re*.cpp 2> nul

echo ESP code generation complete!

if not "%WsEcl2EspPause%" == "no" pause
