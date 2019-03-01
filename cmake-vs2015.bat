@echo off

REM Run this batch from the build directory.
REM
REM Set the git root. All projects should be checkout under this root.
set GIT_ROOT=/git

REM Then we have:
REM c:\git\hpcc
REM c:\git\PlatformExernalsVS2015
REM c:\git\hpcc-bld 

cmake -G "Visual Studio 14 2015 Win64" -DCMAKE_BUILD_TYPE=Debug -DEXTERNALS_DIRECTORY=%GIT_ROOT%/PlatformExternalsVS2015 -DUSE_CBLAS=OFF -DUSE_APR=OFF -DUSE_XALAN=OFF -DUSE_LIBXSLT=ON -DUSE_CASSANDRA=OFF -DWSSQL_SERVICE=OFF –DUSE_CBLAS=OFF %GIT_ROOT%/hpcc