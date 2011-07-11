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


if "%1" == "-h" (
    echo This batch file generates cumulative output for a given query ^(commonly
    echo known as fake output like 'fake roxie'^).  It runs multiple requests for 
    echo the same query and produces a condensed result where multiple XML tags 
    echo and their descendents are merged into one branch (for e.g., a^* as a and 
    echo a[1]/b and a[2]/c are merged as a[1]/[b,c].  This is useful to achieve
    echo a data independent response since some tags might not be produced for 
    echo certain test cases while others might not for yet other test cases.
    echo Some times we are simply interested in finding out which tags are being
    echo produced for which version etc. without caring for data contents especially
    echo for ESPs hitting moxie in the back end since no XML schema is available from
    echo Moxie.
    rem
    echo Instructions:
    rem
    echo 1. set path to include directories where soapplus, xalan.exe and 
    echo    examxml.lnk ^(shortcut to examxml^) reside
    echo 2. change parameters as set in the set instructions below
    echo 3. put multiple requests ^(for the same query^) in fakeEspRequests folder
    echo 4. define esp2 if you would like to compare 2 ESPs, otherwise set esp2 to %esp1%
    echo 5. define ver2 if you would like to compare 2 versions, otherwise set ver2 to %ver1%
    exit /b 0
)

setlocal enabledelayedexpansion
set path=%cd%\binaries;%path%

rem you can raise the following tracing level upto 10 (see soapplus -h for more info)
set tracing=0

set service=WsAccurint
set query=AssetReport

rem set esp1 and esp2 like http://userid:password@url.com:port
set /p password1=Enter password for the first ESP: 
set /p password2=Enter Password for the second ESP: 

set esp1=http://webapp_roxie_test:%password1%$@qawebesp.sc.seisint.com:7170
set esp2=%esp1%

set ver1=1.25
set ver2=%ver1%

set requestDir=fakeEspRequests
set dir1=%query%\esp1
set dir2=%query%\esp2

rem locals:
set esp=%esp1%
set ver=%ver1%
set dir=%dir1%

rem change directory to that of this batch file
for %%D in (%0) do cd /d %%~dpD

:loop
mkdir !dir! 2> nul
echo Processing !dir! ...
echo Fetching XML Schema ...
soapplus -c -d %tracing% -url !esp!/%service%/%query%?xsd^&ver_=!ver! -o _tmp -w
copy _tmp\response_content\get.txt !dir!\%query%.xsd > nul

echo Fetching response XML ...
soapplus -c -d %tracing% -url !esp!/%service%/%query%?respxml_^&ver_=!ver! -o _tmp -w
copy _tmp\response_content\get.txt !dir!\%query%Dummy.xml > nul

echo Coalescing !dir!\%query%.xml ...
xalan -o !dir!\%query%DummyCoalesced.xml !dir!\%query%Dummy.xml coalesce.xslt
md !dir!\coalesced 2> nul

echo Running requests ...
soapplus -c -d %tracing% -url !esp!/%service%?ver_=!ver! -v -xsd !dir!\%query%.xsd -i %requestDir% -o !dir! -w

pushd !dir!

set firstRequest=y
for %%i in (..\..\%requestDir%\*.xml) do (
    set filename=%%~nxi
    set file=response_content\!filename!

    set cfile=coalesced\!filename!

    xalan -o !cfile! !file! ..\..\coalesce.xslt

    if "!firstRequest!" == "y" (
        echo Copying !cfile! to coalesced.xml
        copy !cfile! coalesced.xml > nul
        set first=no
    ) else (
        echo Merging !cfile! file into coalesced.xml...
        xalan -p doc2 'd:\scapps_dev\xmldiffs\!dir!\!cfile!' -o temp.xml coalesced.xml ..\..\coalesceDocs.xslt
        copy temp.xml coalesced.xml > nul
        del temp.xml > nul 2> nul
    )

    if "!dir!" == "%dir2%" examxml.lnk ..\..\%dir1%\!file! !file!
)
popd

if "!esp!" == "%esp1%" (
    set round2=false

    if not "%esp2%" == "%esp1%" set round2=true
    if not "%ver2%" == "%ver1%" set round2=true

    if "%round2%" == "true" (
        set esp=%esp2%
        set ver=%ver2%
        set dir=%dir2%
        goto loop
    )
)

if "!dir!" == "%dir2%" (
    examxml.lnk %dir1%\%query%DummyCoalesced.xml %dir2%\%query%DummyCoalesced.xml
    examxml.lnk %dir1%\coalesced.xml %dir2%\coalesced.xml
)