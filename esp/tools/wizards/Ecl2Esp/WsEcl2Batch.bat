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
rem set rename=

if "%url%" == "" set /p url=Enter URL for WsECL (as http[s]://^<host^>[:^<port^>]):
if "%userid%" == "" set /p userid=Enter UserId:
if "%password%" == "" set /p password=Enter Password (Warning: Password is echoed back!):
if "%module%" == "" set /p module=Enter ECL Module Name:
if "%query%" == "" set /p query=Enter ECL Query Name:

if "%method%" == "" set /p method=Enter ESP Method Name [%query%]:
if "%method%" == "" set method=%query%

if "%rename%" == "" set /p rename=Rename tags in map file? y/n [n]:
if "%rename%" == "" set rename=0
if "%rename%" == "n" set rename=0
if "%rename%" == "y" set rename=1

rem the following defines the name of the dataset in the ECL response with results
rem Note that most queries use the name 'Results'
set dataset=Results
if "%dataset%" == "" set /p dataset=Enter Dataset name in ECL Response [Results]:
if "%dataset%" == "" set dataset=Results

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
echo Filling in missing XML elements in %method%ResponseFull.xml using embedded XML schema and generating %method%Response.xml ...
soapplus -r -ra -g -xsd %method%\%method%ResponseFull.xml -i %method%\%method%ResponseFull.xml -m %method%Response -wiz -d %tracing% -gf %method%\%method%Response.xml

rem generate CustomNames.xml from CustomNames.txt
..\..\..\..\tools\misc\awk -f ini2xml.awk CustomNames.txt > CustomNames.xml

rem generate def files...
set xslfile=xml2batch.xsl
set infile=%method%\%method%Request.xml
set outfile=%method%\%method%Request.def
echo Generating request def file %method%Request.def ...
..\..\..\..\system\xalan\xalan -p method '%method%' -p response 0 -o %outfile% %infile% %xslfile%

set infile=%method%\%method%Response.xml
set outfile=%method%\%method%Response.def
echo Generating response def file %method%Response.def ...
..\..\..\..\system\xalan\xalan -p method '%method%' -p response 1 -p responseDataset '%dataset%' -o %outfile% %infile% %xslfile%

rem generate map files...
set infile=%method%\%method%Request.xml
set outfile=%method%\%method%Request.map
echo Generating request map file %method%Request.map ...
..\..\..\..\system\xalan\xalan -p method '%method%' -p response 0 -p def 0 -p rename %rename% -o %outfile% %infile% %xslfile%

set infile=%method%\%method%Response.xml
set outfile=%method%\%method%Response.map
echo Generating response map file %method%Response.map ...
..\..\..\..\system\xalan\xalan -p method '%method%' -p response 1 -p def 0 -p rename %rename% -p responseDataset '%dataset%'  -o %outfile% %infile% %xslfile%

echo Batch code generation complete!

if not "%WsEcl2EspPause%" == "no" pause
