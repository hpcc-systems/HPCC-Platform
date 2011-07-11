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
