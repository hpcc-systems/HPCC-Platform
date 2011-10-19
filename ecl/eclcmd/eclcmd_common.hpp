/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifndef ECLCMD_COMMON_HPP
#define ECLCMD_COMMON_HPP

//=========================================================================================

interface IEclCommand : extends IInterface
{
    virtual bool parseCommandLineOptions(ArgvIterator &iter)=0;
    virtual void finalizeOptions(IProperties *globals)=0;
    virtual int processCMD()=0;
    virtual void usage()=0;
};

typedef IEclCommand *(*EclCommandFactory)(const char *cmdname);

#define ECLOPT_SERVER "--server"
#define ECLOPT_SERVER_INI "eclWatchIP"
#define ECLOPT_SERVER_ENV "ECL_WATCH_IP"

#define ECLOPT_PORT "--port"
#define ECLOPT_PORT_INI "eclWatchPort"
#define ECLOPT_PORT_ENV "ECL_WATCH_PORT"

#define ECLOPT_USERNAME "--username"
#define ECLOPT_USERNAME_INI "eclUserName"
#define ECLOPT_USERNAME_ENV "ECL_USER_NAME"

#define ECLOPT_PASSWORD "--password"
#define ECLOPT_PASSWORD_INI "eclPassword"
#define ECLOPT_PASSWORD_ENV "ECL_PASSWORD"

#define ECLOPT_ACTIVATE "--activate"
#define ECLOPT_ACTIVATE_INI "activateDefault"
#define ECLOPT_ACTIVATE_ENV NULL

#define ECLOPT_WUID "--wuid"
#define ECLOPT_CLUSTER "--cluster"
#define ECLOPT_NAME "--name"
#define ECLOPT_ACTIVATE "--activate"
#define ECLOPT_VERSION "--version"

bool extractOption(StringBuffer & option, IProperties * globals, const char * envName, const char * propertyName, const char * defaultPrefix, const char * defaultSuffix);
bool extractOption(StringAttr & option, IProperties * globals, const char * envName, const char * propertyName, const char * defaultPrefix, const char * defaultSuffix);
bool extractOption(bool & option, IProperties * globals, const char * envName, const char * propertyName, bool defval);

enum eclObjParameterType
{
    eclObjTypeUnknown = 0x00,
    eclObjSource = 0x01,
    eclObjArchive = 0x02,
    eclObjSharedObject = 0x04,
    eclObjWuid = 0x08,
    eclObjQueryId = 0x10
};

class EclObjectParameter
{
public:
    EclObjectParameter(unsigned _accept=0x00);
    eclObjParameterType set(const char *_value);
    const char *queryTypeName();
    StringBuffer &getDescription(StringBuffer &s);

public:
    eclObjParameterType type;
    StringAttr value;
    unsigned accept;
};

class EclCmdCommon : public CInterface, implements IEclCommand
{
public:
    IMPLEMENT_IINTERFACE;
    EclCmdCommon()
    {
    }
    virtual bool matchCommandLineOption(ArgvIterator &iter)
    {
        if (iter.matchOption(optServer, ECLOPT_SERVER))
            return true;
        if (iter.matchOption(optPort, ECLOPT_PORT))
            return true;
        if (iter.matchOption(optUsername, ECLOPT_USERNAME))
            return true;
        if (iter.matchOption(optPassword, ECLOPT_PASSWORD))
            return true;
        return false;
    }
    virtual void finalizeOptions(IProperties *globals)
    {
        extractOption(optServer, globals, ECLOPT_SERVER_ENV, ECLOPT_SERVER_INI, ".", NULL);
        extractOption(optPort, globals, ECLOPT_PORT_ENV, ECLOPT_PORT_INI, "8010", NULL);
        extractOption(optUsername, globals, ECLOPT_USERNAME_ENV, ECLOPT_USERNAME_INI, NULL, NULL);
        extractOption(optPassword, globals, ECLOPT_PASSWORD_ENV, ECLOPT_PASSWORD_INI, NULL, NULL);
    }
    virtual void usage()
    {
        fprintf(stdout,
            "      --server=<ip>        ip of server running ecl services (eclwatch)\n"
            "      --port=<port>        ecl services port\n"
            "      --username=<name>    username for accessing ecl services\n"
            "      --password=<pw>      password for accessing ecl services\n"
        );
    }
public:
    StringAttr optServer;
    StringAttr optPort;
    StringAttr optUsername;
    StringAttr optPassword;
};

#endif
