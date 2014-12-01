/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

#include "jliball.hpp"
#include "esdl_def.hpp"
#include "esdlcmd_common.hpp"

#include "esdlcmd_core.hpp"
#include "build-config.h"


class EsdlPublishCmdCommon : public EsdlCmdCommon
{
protected:
    StringAttr optSource;
    StringAttr optService; //RODRIGO, which service??? ESP Service? ESDL service? they could be one and the same, not sure
    StringAttr optWSProcAddress;
    StringAttr optWSProcPort;
    StringAttr optVersionStr;
    double     optVersion;
    StringAttr optUser;
    StringAttr optPass;
    StringAttr optESDLDefID;
    StringAttr optESDLService;

    StringAttr optTargetESPProcName;
    StringAttr optTargetAddress;
    StringAttr optTargetPort;
    bool       optOverWrite;

    Owned<EsdlCmdHelper> esdlHelper;

public:
    EsdlPublishCmdCommon()
    {
        esdlHelper.setown(EsdlCmdHelper::createEsdlHelper());
    }

    virtual int processCMD() = 0;

    virtual bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
        {
            usage();
            return false;
        }

        for (; !iter.done(); iter.next())
        {
            if (parseCommandLineOption(iter))
                continue;

            if (matchCommandLineOption(iter, true)!=EsdlCmdOptionMatch)
                return false;
        }

        return true;
    }

    virtual void usage()
    {
         printf(
                "   -s, --server <ip>            IP of server running ESDL services\n"
                "   --port <port>                ESDL services port\n"
                "   -u, --username <name>        Username for accessing ESDL services\n"
                "   -pw, --password <pw>         Password for accessing ESDL services\n"
                "   --version <ver>              ESDL service version\n"
                );
        EsdlCmdCommon::usage();
    }

    virtual bool parseCommandLineOption(ArgvIterator &iter)
    {
        if (iter.matchFlag(optSource, ESDL_CONVERT_SOURCE))
            return true;
        if (iter.matchFlag(optService, ESDLOPT_SERVICE))
            return true;
        if (iter.matchFlag(optWSProcAddress, ESDL_OPT_SERVICE_SERVER) || iter.matchFlag(optWSProcAddress, ESDL_OPTION_SERVICE_SERVER))
            return true;
        if (iter.matchFlag(optVersionStr, ESDLOPT_VERSION))
            return true;
        if (iter.matchFlag(optUser, ESDL_OPT_SERVICE_USER) || iter.matchFlag(optUser, ESDL_OPTION_SERVICE_USER))
            return true;
        if (iter.matchFlag(optPass, ESDL_OPT_SERVICE_PASS) || iter.matchFlag(optPass, ESDL_OPTION_SERVICE_PASS))
            return true;
        if (iter.matchFlag(optWSProcPort, ESDL_OPTION_SERVICE_PORT) || iter.matchFlag(optWSProcPort, ESDL_OPT_SERVICE_PORT))
            return true;
        if (iter.matchFlag(optOverWrite, ESDL_OPTION_OVERWRITE) )
            return true;

        return false;
    }

    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!optVersionStr.isEmpty())
        {
            optVersion = atof( optVersionStr.get() );
            if( optVersion <= 0 )
            {
                throw MakeStringException( 0, "Version option must be followed by a real number > 0" );
            }
        }
        else
        {
            fprintf(stderr, "\nWARNING: ESDL Version not specified.\n");
        }

        if (optSource.isEmpty())
            throw MakeStringException( 0, "Source ESDL XML definition file must be provided" );

        if (optService.isEmpty())
            throw MakeStringException( 0, "Name of ESDL based service must be provided" );

        if (optWSProcAddress.isEmpty())
            throw MakeStringException( 0, "Server address of ESDL process server must be provided" );

        if (optWSProcPort.isEmpty())
            throw MakeStringException( 0, "Port on which ESDL process is listening must be provided" );

        return true;
    }
};

class EsdlPublishCmd : public EsdlPublishCmdCommon
{
public:
    int processCMD()
    {
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientPublishESDLDefinitionRequest> request = esdlConfigClient->createPublishESDLDefinitionRequest();

        StringBuffer esxml;
        esdlHelper->getServiceESXDL(optSource.get(), optESDLService.get(), esxml, optVersion);

        if (esxml.length()==0)
        {
            fprintf(stderr,"\nESDL Definition for service %s could not be loaded from: %s\n", optESDLService.get(), optSource.get());
            return -1;
        }

        request->setServiceName(optESDLService.get());

        if (optVerbose)
            fprintf(stderr,"\nESDL Definition: \n%s\n", esxml.str());

        request->setXMLDefinition(esxml.str());
        request->setDeletePrevious(optOverWrite);

        Owned<IClientPublishESDLDefinitionResponse> resp = esdlConfigClient->PublishESDLDefinition(request);

        bool validateMessages = false;
        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        fprintf(stdout, "\n %s. Service Name: %s, Version: %d", resp->getStatus().getDescription(),resp->getServiceName(),resp->getEsdlVersion());

        return 0;
    }

    void usage()
    {
        printf("\nUsage:\n\n"
                "esdl publish <servicename> <filename.ecm> [command options]\n\n"
                "   <servicename>        The ESDL defined ESP service to publish\n"
                "   <filename.ecm>       The ESDL file containing service definition\n"
                "Options (use option flag followed by appropriate value):\n"
                "   --overwrite                    Overwrite the latest version of this ESDL Definition\n"
                );

        EsdlPublishCmdCommon::usage();
    }

    bool parseCommandLineOptions(ArgvIterator &iter)
    {
       if (iter.done())
       {
           usage();
           return false;
       }

       //First x parameter's order is fixed.
       //<servicename>
       //<filename.ecm>       The ESDL file containing service definition\n"
       for (int cur = 0; cur < 2 && !iter.done(); cur++)
       {
          const char *arg = iter.query();
          if (*arg != '-')
          {
              switch (cur)
              {
               case 0:
                   optESDLService.set(arg);
                   break;
               case 1:
                   optSource.set(arg);
                   break;
               default:
                   fprintf(stderr, "\nUnrecognized positional argument detected : %s\n", arg);
                   usage();
                   return false;
              }
          }
          else
          {
              fprintf(stderr, "\nOption detected before required arguments: %s\n", arg);
              usage();
              return false;
          }

          iter.next();
       }

       for (; !iter.done(); iter.next())
       {
           if (parseCommandLineOption(iter))
               continue;

           if (matchCommandLineOption(iter, true)!=EsdlCmdOptionMatch)
               return false;
       }

       return true;
    }

    bool finalizeOptions(IProperties *globals)
    {
        if (!optVersionStr.isEmpty())
        {
            optVersion = atof( optVersionStr.get() );
            if( optVersion <= 0 )
            {
                throw MakeStringException( 0, "Version option must be followed by a real number > 0" );
            }
        }
        else
        {
            fprintf(stderr, "\nWARNING: ESDL Version not specified.\n");
        }

        if (optSource.isEmpty())
            throw MakeStringException( 0, "Source ESDL XML definition file must be provided" );

        if (optESDLService.isEmpty())
            throw MakeStringException( 0, "Name of ESDL based service must be provided" );

        if (optWSProcAddress.isEmpty())
            throw MakeStringException( 0, "Server address of ESDL process server must be provided" );

        if (optWSProcPort.isEmpty())
            throw MakeStringException( 0, "Port on which ESDL process is listening must be provided" );

        return true;
    }
};

class EsdlBindServiceCmd : public EsdlPublishCmdCommon
{
protected:
    StringAttr optPortOrName;
    StringAttr optInput;

public:
    int processCMD()
    {
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientPublishESDLBindingRequest> request = esdlConfigClient->createPublishESDLBindingRequest();

        fprintf(stderr,"\nAttempting to configure ESDL Service: '%s'\n", optESDLService.get());

        request->setEspProcName(optTargetESPProcName);
        request->setEspPort(optTargetPort);
        request->setEspServiceName(optService);
        request->setEsdlServiceName(optESDLService);
        request->setEsdlDefinitionID(optESDLDefID);
        request->setConfig(optInput);
        request->setOverwrite(optOverWrite);

        if (optVerbose)
            fprintf(stderr,"\nMethod config: %s\n", optInput.get());

        //Owned<IClientConfigureESDLServiceResponse> resp = esdlConfigClient->ConfigureESDLService(request);
        Owned<IClientPublishESDLBindingResponse> resp = esdlConfigClient->PublishESDLBinding(request);

        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        fprintf(stdout, "\n %s.", resp->getStatus().getDescription());

        return 0;
    }

    void usage()
    {
        printf( "\nUsage:\n\n"
                "esdl bind-service <TargetESPProcessName> <TargetESPBindingPort | TargetESPServiceName> <ESDLDefinitionId> <ESDLServiceName> [command options]\n\n"
                "   TargetESPProcessName                             The target ESP Process name\n"
                "   TargetESPBindingPort | TargetESPServiceName      Either target ESP binding port or target ESP service name\n"
                "   ESDLDefinitionId                           The Name and version of the ESDL definition to bind to this service (must already be defined in dali.)\n"
                "   ESDLServiceName                                  The Name of the ESDL Service (as defined in the ESDL Definition)\n"

                "\nOptions (use option flag followed by appropriate value):\n"
                "   --config <file|xml>                              Configuration XML for all methods associated with the target Service\n"
                "   --overwrite                                      Overwrite binding if it already exists\n");

                EsdlPublishCmdCommon::usage();
//RODRIGO update this usage
        printf( "\n Use this command to publish ESDL Service based bindings.\n"
                "   To bind an ESDL Service, provide the target ESP process name\n"
                "   (ESP Process which will host the ESP Service as defined in the ESDL Definition.) \n"
                "   It is also necessary to provide the Port on which this service is configured to run (ESP Binding),\n"
                "   and the name of the service you are binding.\n"
                "   Optionally provide configuration information either directly, or via a\n"
                "   configuration file in the following syntax:\n"
                "     <Methods>\n"
                "     \t<Method name=\"myMethod\" url=\"http://10.45.22.1:292/somepath?someparam=value\" user=\"myuser\" password=\"mypass\"/>\n"
                "     \t<Method name=\"myMethod2\" url=\"http://10.45.22.1:292/somepath?someparam=value\" user=\"myuser\" password=\"mypass\"/>\n"
                "     </Methods>\n"
                );

        printf("\nExample:"
                ">esdl bind-service myesp 8088 WsMyService --config /myService/methods.xml\n"
                );
    }

    bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
        {
            usage();
            return false;
        }

        //First 4 parameter's order is fixed.
        //TargetESPProcessName
        //TargetESPBindingPort | TargetESPServiceName
        //ESDLDefinitionId
        //ESDLServiceName
        for (int cur = 0; cur < 4 && !iter.done(); cur++)
        {
           const char *arg = iter.query();
           if (*arg != '-')
           {
               switch (cur)
               {
                case 0:
                    optTargetESPProcName.set(arg);
                    break;
                case 1:
                    optPortOrName.set(arg);
                    break;
                case 2:
                    optESDLDefID.set(arg);
                    break;
                case 3:
                    optESDLService.set(arg);
                    break;
                default:
                    fprintf(stderr, "\nUnrecognized positional argument detected : %s\n", arg);
                    usage();
                    return false;
               }
           }
           else
           {
               fprintf(stderr, "\nOption detected before required arguments: %s\n", arg);
               usage();
               return false;
           }

           iter.next();
        }

        for (; !iter.done(); iter.next())
        {
            if (parseCommandLineOption(iter))
                continue;

            if (matchCommandLineOption(iter, true)!=EsdlCmdOptionMatch)
                return false;
        }

        return true;
    }

    bool parseCommandLineOption(ArgvIterator &iter)
    {
        if (iter.matchFlag(optInput, ESDL_OPTION_CONFIG) )
            return true;
        //if (iter.matchFlag(optOverWrite, ESDL_OPTION_OVERWRITE) )
        //    return true;

        if (EsdlPublishCmdCommon::parseCommandLineOption(iter))
            return true;

        return false;
    }

    bool finalizeOptions(IProperties *globals)
    {
        if (optInput.length())
        {
            const char *in = optInput.get();
            while (*in && isspace(*in)) in++;
            if (*in!='<')
            {
                StringBuffer content;
                content.loadFile(in);
                optInput.set(content.str());
            }
        }

        if (optESDLDefID.isEmpty())
            throw MakeStringException( 0, "ESDL definition ID must be provided!" );

        if (optESDLService.isEmpty())
            throw MakeStringException( 0, "ESDL service definition name must be provided!" );

        if(optTargetESPProcName.isEmpty())
            throw MakeStringException( 0, "Name of Target ESP process must be provided!" );

        if (optPortOrName.isEmpty())
            throw MakeStringException( 0, "Either the target ESP service port of name must be provided!" );
        else
        {
            const char * portorname =  optPortOrName.get();
            isdigit(*portorname) ? optTargetPort.set(portorname) : optService.set(portorname);
        }

        if (optWSProcAddress.isEmpty())
            throw MakeStringException( 0, "Server address of ESDL process server must be provided" );

        if (optWSProcPort.isEmpty())
            throw MakeStringException( 0, "Port on which ESDL process is listening must be provided" );

        return true;
    }
};

class EsdlBindMethodCmd : public EsdlBindServiceCmd
{
protected:
    StringAttr optMethod;
    StringAttr optBindingName;

public:
    int processCMD()
    {
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientConfigureESDLBindingMethodRequest> request = esdlConfigClient->createConfigureESDLBindingMethodRequest();

        fprintf(stderr,"\nAttempting to configure Method : '%s'.'%s'\n", optService.get(), optMethod.get());
        request->setEspProcName(optTargetESPProcName);
        request->setEspBindingName(optBindingName);
        request->setEsdlServiceName(optService.get());
        VStringBuffer id("%s.%d", optService.get(), (int)optVersion);
        request->setEsdlDefinitionID(id.str());
        request->setConfig(optInput);
        request->setOverwrite(optOverWrite);

        if (optVerbose)
            fprintf(stderr,"\nMethod config: %s\n", optInput.get());

        Owned<IClientConfigureESDLBindingMethodResponse> resp = esdlConfigClient->ConfigureESDLBindingMethod(request);

        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        fprintf(stdout, "\n %s.", resp->getStatus().getDescription());

        return 0;
    }

    void usage()
    {
        printf( "\nUsage:\n\n"
                "esdl bind-method <TargetESPProcessName> <TargetESPBindingName> <TargetServiceName> <TargetServiceDefVersion> <TargetMethodName> [command options]\n\n"
                "   TargetESPProcessName                             The target ESP Process name\n"
                "   TargetESPBindingName                             The target ESP binding name associated with this service\n"
                "   TargetServiceName                                The name of the Service to bind (must already be defined in dali.)\n"
                "   TargetServiceDefVersion                          The version of the target service ESDL definition (must exist in dali)\n"
                "   TargetMethodName                                 The name of the target method (must exist in the service ESDL definition)\n"

                "\nOptions (use option flag followed by appropriate value):\n"
                "   --config <file|xml>                              Configuration XML for all methods associated with the target Service\n"
                "   --overwrite                                      Overwrite binding if it already exists\n");

                EsdlPublishCmdCommon::usage();

        printf( "\n Use this command to publish ESDL Service based bindings.\n"
                "   To bind a ESDL Service, provide the target ESP process name\n"
                "   (esp which will host the service.) \n"
                "   It is also necessary to provide the Port on which this service is configured to run (ESP Binding),\n"
                "   and the name of the service you are binding.\n"
                "   Optionally provide configuration information either directly, or via a\n"
                "   configuration file in the following syntax:\n"
                "     <Methods>\n"
                "     \t<Method name=\"myMethod\" url=\"http://10.45.22.1:292/somepath?someparam=value\" user=\"myuser\" password=\"mypass\"/>\n"
                "     \t<Method name=\"myMethod2\" url=\"http://10.45.22.1:292/somepath?someparam=value\" user=\"myuser\" password=\"mypass\"/>\n"
                "     </Methods>\n"
                );

        printf("\nExample:"
                ">esdl bind-service myesp 8088 WsMyService --config /myService/methods.xml\n"
                );
    }

    bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
        {
            usage();
            return false;
        }

        //First 5 parameter order is fixed.
        for (int cur = 0; cur < 5 && !iter.done(); cur++)
        {
           const char *arg = iter.query();
           if (*arg != '-')
           {
               switch (cur)
               {
                case 0:
                    optTargetESPProcName.set(arg);
                    break;
                case 1:
                    optBindingName.set(arg);
                    break;
                case 2:
                    optService.set(arg);
                    break;
                case 3:
                    optVersionStr.set(arg);
                    break;
                case 4:
                    optMethod.set(arg);
                    break;
                default:
                    fprintf(stderr, "\nUnrecognized positional argument detected : %s\n", arg);
                    usage();
                    return false;
               }
           }
           else
           {
               fprintf(stderr, "\noption detected before required arguments: %s\n", arg);
               usage();
               return false;
           }

           iter.next();
        }

        for (; !iter.done(); iter.next())
        {
            if (parseCommandLineOption(iter))
                continue;

            if (matchCommandLineOption(iter, true)!=EsdlCmdOptionMatch)
                return false;
        }

        return true;
    }

    bool finalizeOptions(IProperties *globals)
    {
        if (optInput.length())
        {
            const char *in = optInput.get();
            while (*in && isspace(*in)) in++;
            if (*in!='<')
            {
                StringBuffer content;
                content.loadFile(in);
                optInput.set(content.str());
            }
        }

        if (!optVersionStr.isEmpty())
        {
            optVersion = atof( optVersionStr.get() );
            if( optVersion <= 0 )
            {
                throw MakeStringException( 0, "Version option must be followed by a real number > 0" );
            }
        }
        else
            throw MakeStringException( 0, "ESDL service definition version must be provided!" );

        if(optTargetESPProcName.isEmpty())
            throw MakeStringException( 0, "Name of Target ESP process must be provided" );

        if (optService.isEmpty())
            throw MakeStringException( 0, "Name of ESDL based service must be provided" );

        if (optWSProcAddress.isEmpty())
            throw MakeStringException( 0, "Server address of ESDL process server must be provided" );

        if (optWSProcPort.isEmpty())
            throw MakeStringException( 0, "Port on which ESDL process is listening must be provided" );

        if (optMethod.isEmpty())
            throw MakeStringException( 0, "Name of ESDL based method must be provided" );

        if (optBindingName.isEmpty())
            throw MakeStringException( 0, "Name of ESP binding must be provided" );

        return true;
    }
};

class EsdlGetCmd : public EsdlPublishCmdCommon
{
    protected:
        StringAttr optId;
    public:
        bool parseCommandLineOptions(ArgvIterator &iter)
           {
               if (iter.done())
               {
                   usage();
                   return false;
               }

               //First parameter is fixed.
               for (int cur = 0; cur < 1 && !iter.done(); cur++)
               {
                  const char *arg = iter.query();
                  if (*arg != '-')
                  {
                      switch (cur)
                      {
                       case 0:
                           optId.set(arg);
                           break;
                       default:
                           fprintf(stderr, "\nUnrecognized positional argument detected : %s\n", arg);
                           usage();
                           return false;
                      }
                  }
                  else
                  {
                      fprintf(stderr, "\noption detected before required arguments: %s\n", arg);
                      usage();
                      return false;
                  }

                  iter.next();
               }

               for (; !iter.done(); iter.next())
               {
                   if (parseCommandLineOption(iter))
                       continue;

                   if (matchCommandLineOption(iter, true)!=EsdlCmdOptionMatch)
                       return false;
               }

               return true;
           }

        bool parseCommandLineOption(ArgvIterator &iter)
        {
            if (iter.matchFlag(optTargetAddress, ESDL_OPTION_TARGET_ESP_ADDRESS))
                return true;
            if (iter.matchFlag(optTargetPort, ESDL_OPTION_TARGET_ESP_PORT) )
                return true;

            if (EsdlPublishCmdCommon::parseCommandLineOption(iter))
                return true;

            return false;
        }

        bool finalizeOptions(IProperties *globals)
        {
            if (optWSProcAddress.isEmpty())
                throw MakeStringException( 0, "Server address of WsESDLConfig server must be provided" );

            if (optWSProcPort.isEmpty())
                throw MakeStringException( 0, "Port on which WsESDLConfig is listening must be provided" );

            if (optId.isEmpty())
                throw MakeStringException( 0, "ESDL ID must be provided" );

            return true;
        }
};

class EsdlGetDefinitionCmd : public EsdlGetCmd
{
    public:
        int processCMD()
        {
            Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optWSProcAddress, optWSProcPort, optUser, optPass);
            Owned<IClientGetESDLDefinitionRequest> request = esdlConfigClient->createGetESDLDefinitionRequest();

            fprintf(stdout,"\nAttempting to get ESDL definition: %s\n", optId.get());

            request->setId(optId);

            Owned<IClientGetESDLDefinitionResponse> resp = esdlConfigClient->GetESDLDefinition(request);

            if (resp->getExceptions().ordinality()>0)
            {
                EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
                return 1;
            }

            fprintf(stdout, "\n%s", resp->getXMLDefinition());
            fprintf(stdout, "\n%s.", resp->getStatus().getDescription());

            return 0;
        }

        void usage()
        {

            printf( "\nUsage:\n\n"
                    "esdl get-definition <ESDLDefinitionID> [command options]\n\n"
                    "Options (use option flag followed by appropriate value):\n"
                    "   ESDLDefinitionID                             The ESDL Definition id <esdldefname>.<esdldefver>\n"
                   );

                    EsdlPublishCmdCommon::usage();

            printf( "\n\n Use this command to get DESDL definitions.\n"
                    " To specify the target DESDL based service configuration, provide the target ESP process \n"
                    " (esp process name or machine IP Address) which hosts the service.\n"
                    " It is also necessary to provide the Port on which this service is configured to run,\n"
                    " and the name of the service.\n"
                    );
        }
};

class EsdlGetBindingCmd : public EsdlGetCmd
{
public:
    int processCMD()
    {
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientGetESDLBindingRequest> request = esdlConfigClient->createGetESDLBindingRequest();

        fprintf(stdout,"\nAttempting to get ESDL binding: %s\n", optId.get());

        request->setEsdlBindingId(optId);

        Owned<IClientGetESDLBindingResponse> resp = esdlConfigClient->GetESDLBinding(request);

        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        fprintf(stdout, "\n%s", resp->getConfigXML());
        fprintf(stdout, "\n%s.", resp->getStatus().getDescription());

        return 0;
    }

    void usage()
    {

        printf( "\nUsage:\n\n"
                "esdl get-binding <ESDLBindingId> [command options]\n\n"
                "Options (use option flag followed by appropriate value):\n"
                "   ESDLBindingId                             The target ESDL binding id <espprocessname>.<espbindingname>\n"
               );

                EsdlPublishCmdCommon::usage();

        printf( "\n\n Use this command to get DESDL Service based bindings.\n"
                " To specify the target DESDL based service configuration, provide the target ESP process \n"
                " (esp process name or machine IP Address) which hosts the service.\n"
                " It is also necessary to provide the Port on which this service is configured to run,\n"
                " and the name of the service.\n"
                );
    }
};
