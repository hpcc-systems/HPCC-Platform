@echo off
rem /*##############################################################################
rem 
rem     HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
rem 
rem     Licensed under the Apache License, Version 2.0 (the "License");
rem     you may not use this file except in compliance with the License.
rem     You may obtain a copy of the License at
rem 
rem        http://www.apache.org/licenses/LICENSE-2.0
rem 
rem     Unless required by applicable law or agreed to in writing, software
rem     distributed under the License is distributed on an "AS IS" BASIS,
rem     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem     See the License for the specific language governing permissions and
rem     limitations under the License.
rem ############################################################################## */


set syntax=no
if "%1" == "-h" set syntax=yes
if "%2" == "" set syntax=yes

if "%syntax%" == "yes" (
    echo This batch file copies out soapplus.exe and/or xalan.exe along with 
    echo their dependencies to the specified directory.
    echo getBinaries ^<srcRoot^> ^<destDir^>
    echo for e.g., getBinaries \scapps_dev\project\newecl .\binaries.
    exit /b 0
)

setlocal enabledelayedexpansion

set srcRoot=%1
set dest=%2
set release=debug

mkdir %dest% 2> nul

set xalandir=%srcRoot%\system\xalan
set suffix=

if "%release%" == "debug" set suffix=D

:ask
set /p resp=Would you like to get soapplus and its dependencies? [y/n]: 
if "%resp%" == "" goto ask
if not "%resp%" == "y" goto xalan

for %%i in (jlib.dll xmllib.dll securesocket.dll soapplus.exe) do (
    echo copy %srcRoot%\bin\%release%\%%i %dest%
    copy %srcRoot%\bin\%release%\%%i %dest%
)

for %%i in (libeay32 ssleay32) do (
    echo copy %srcRoot%\system\openssl\out32dll\%%i.dll %dest%
    copy %srcRoot%\system\openssl\out32dll\%%i.dll %dest%
)

for %%i in (Xalan-C_1_8 XalanMessages_1_8) do (
    echo copy %xalandir%\bin\%%i%suffix%.dll %dest%
    copy %xalandir%\xalan-c\bin\%%i%suffix%.dll %dest%
)

echo copy %xalandir%\xerces-c_2_5_0%suffix%.dll %dest%
copy %xalandir%\xerces-c\bin\xerces-c_2_5_0%suffix%.dll %dest%

PATH=binaries;%PATH%
soapplus -h

:xalan
set resp=
set /p resp=Would you like to get xalan and its dependencies? [y/n]: 
if "%resp%" == "" goto xalan
if not "%resp%" == "y" goto done


for %%i in (DOMSupport PlatformSupport XalanDOM XalanExtensions) do (
    echo copy %xalandir%\%%i.dll %dest%
    copy %xalandir%\%%i.dll %dest%
)

for %%i in (XalanSourceTree XalanTransformer XMLSupport XPath XSLT XercesParserLiaison) do (
    echo copy %xalandir%\%%i.dll %dest%
    copy %xalandir%\%%i.dll %dest%
)

echo copy %xalandir%\xerces-c_1_6_0.dll %dest%
copy %xalandir%\xerces-c_1_6_0.dll %dest%

echo copy %xalandir%\xalan.exe %dest%\xalan.exe
copy %xalandir%\xalan.exe %dest%\xalan.exe

for %%i in (Xalan-C_1_8 XalanMessages_1_8) do (
    if not exist %dest%\%%i.dll (
        echo copy %xalandir%\bin\%%i.dll %dest%
        copy %xalandir%\xalan-c\bin\%%i.dll %dest%
    )
)

binaries\xalan
:done
