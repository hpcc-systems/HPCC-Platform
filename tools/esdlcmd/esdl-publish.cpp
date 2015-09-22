/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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
    StringAttr optService;
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
    EsdlPublishCmdCommon() : optWSProcAddress("."), optWSProcPort("8010")
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
        if (iter.matchFlag(optWSProcPort, ESDL_OPTION_SERVICE_PORT) || iter.matchFlag(optWSProcPort, ESDL_OPT_SERVICE_PORT))
            return true;
        if (iter.matchFlag(optVersionStr, ESDLOPT_VERSION))
            return true;
        if (iter.matchFlag(optUser, ESDL_OPT_SERVICE_USER) || iter.matchFlag(optUser, ESDL_OPTION_SERVICE_USER))
            return true;
        if (iter.matchFlag(optPass, ESDL_OPT_SERVICE_PASS) || iter.matchFlag(optPass, ESDL_OPTION_SERVICE_PASS))
            return true;
        if (iter.matchFlag(optOverWrite, ESDL_OPTION_OVERWRITE) )
            return true;

        return false;
    }

    virtual bool finalizeOptions(IProperties *globals)
    {
        if (optWSProcAddress.isEmpty())
            throw MakeStringException( 0, "Server address of WsESDLConfig process server must be provided" );

        if (optWSProcPort.isEmpty())
            throw MakeStringException( 0, "Port on which WsESDLConfig is listening must be provided" );

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
            fprintf(stdout,"\nESDL Definition: \n%s\n", esxml.str());

        request->setXMLDefinition(esxml.str());
        request->setDeletePrevious(optOverWrite);

        Owned<IClientPublishESDLDefinitionResponse> resp = esdlConfigClient->PublishESDLDefinition(request);

        bool validateMessages = false;
        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        fprintf(stdout, "\n %s. Service Name: %s, Version: %d\n", resp->getStatus().getDescription(),resp->getServiceName(),resp->getEsdlVersion());

        return 0;
    }

    void usage()
    {
        printf("\nUsage:\n\n"
                "esdl publish <servicename> <filename.(ecm|esdl)> [command options]\n\n"
                "   <servicename>               The ESDL defined ESP service to publish\n"
                "   <filename.(ecm|esdl|xml)>   The ESDL file containing service definition in esdl format (.ecm |.esdl) or in esxdl format (.xml) \n"
                "Options (use option flag followed by appropriate value):\n"
                "   --overwrite                 Overwrite the latest version of this ESDL Definition\n"
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
       //<filename.(ecm|esdl|xml)>       The ESDL file containing service definition\n"
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

        return EsdlPublishCmdCommon::finalizeOptions(globals);
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

        fprintf(stdout,"\nAttempting to configure ESDL Service: '%s'\n", optESDLService.get());

        request->setEspProcName(optTargetESPProcName);
        request->setEspPort(optTargetPort);
        request->setEspServiceName(optService);
        request->setEsdlServiceName(optESDLService);
        request->setEsdlDefinitionID(optESDLDefID);
        request->setConfig(optInput);
        request->setOverwrite(optOverWrite);

        if (optVerbose)
            fprintf(stdout,"\nMethod config: %s\n", optInput.get());

        Owned<IClientPublishESDLBindingResponse> resp = esdlConfigClient->PublishESDLBinding(request);

        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        fprintf(stdout, "\n %s.\n", resp->getStatus().getDescription());

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

        return EsdlPublishCmdCommon::finalizeOptions(globals);
    }
};

class EsdlUnBindServiceCmd : public EsdlPublishCmdCommon
{
protected:
    StringAttr optEspBinding;

public:
    int processCMD()
    {
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientDeleteESDLBindingRequest> request = esdlConfigClient->createDeleteESDLBindingRequest();

        fprintf(stdout,"\nAttempting to un-bind ESDL Service: '%s.%s'\n", optTargetESPProcName.get(), optEspBinding.get());

        StringBuffer id;
        id.setf("%s.%s", optTargetESPProcName.get(), optEspBinding.get());
        request->setId(id);

        Owned<IClientDeleteESDLRegistryEntryResponse> resp = esdlConfigClient->DeleteESDLBinding(request);

        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        fprintf(stdout, "\n %s.\n", resp->getStatus().getDescription());

        return 0;
    }

    void usage()
    {
        printf( "\nUsage:\n\n"
                "esdl unbind-service <TargetESPProcessName> <TargetESPBindingPort | TargetESPServiceName> [command options]\n\n"
                "   TargetESPProcessName                             The target ESP Process name\n"
                "   TargetESPBindingPort | TargetESPServiceName      Either target ESP binding port or target ESP service name\n");

        EsdlPublishCmdCommon::usage();

        printf( "\n   Use this command to unpublish ESDL Service based bindings.\n"
                "   To unbind an ESDL Service, provide the target ESP process name\n"
                "   (ESP Process which will host the ESP Service as defined in the ESDL Definition.) \n"
                "   It is also necessary to provide the Port on which this service is configured to run (ESP Binding),\n"
                "   and the name of the service you are unbinding.\n"
                );

        printf("\nExample:"
                ">esdl unbind-service myesp 8088 \n"
                );
    }

    bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
        {
            usage();
            return false;
        }

        for (int cur = 0; cur < 2 && !iter.done(); cur++)
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
                    optEspBinding.set(arg);
                    break;
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
        if (EsdlPublishCmdCommon::parseCommandLineOption(iter))
            return true;

        return false;
    }

    bool finalizeOptions(IProperties *globals)
    {

        if (optTargetESPProcName.isEmpty())
            throw MakeStringException( 0, "Name of Target ESP process must be provided!" );

        if (optEspBinding.isEmpty())
            throw MakeStringException( 0, "Target ESP Binding must be provided!" );

        return EsdlPublishCmdCommon::finalizeOptions(globals);
    }
};

class EsdlDeleteESDLDefCmd : public EsdlPublishCmdCommon
{
public:
    int processCMD()
    {
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientDeleteESDLDefinitionRequest> request = esdlConfigClient->createDeleteESDLDefinitionRequest();

        fprintf(stdout,"\nAttempting to delete ESDL definition: '%s.%d'\n", optESDLService.get(), (int)optVersion);

        StringBuffer id;
        id.setf("%s.%d", optESDLService.get(), (int)optVersion);

        request->setId(id);

        Owned<IClientDeleteESDLRegistryEntryResponse> resp = esdlConfigClient->DeleteESDLDefinition(request);

        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        fprintf(stdout, "\n %s.\n", resp->getStatus().getDescription());

        return 0;
    }

    void usage()
    {
        printf( "\nUsage:\n\n"
                "esdl delete <ESDLServiceDefinitionName> <ESDLServiceDefinitionVersion> [command options]\n\n"
                "   ESDLServiceDefinitionName         The name of the ESDL service definition to delete\n"
                "   ESDLServiceDefinitionVersion      The version of the ESDL service definition to delete\n");

        EsdlPublishCmdCommon::usage();

        printf( "\n   Use this command to delete an ESDL Service definition.\n"
                "   To delete an ESDL Service definition, provide the definition name and version\n"
                );

        printf("\nExample:"
                ">esdl delete myesdldef 5\n"
                );
    }

    bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
        {
            usage();
            return false;
        }

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
                    optVersionStr.set(arg);
                    break;
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
        if (EsdlPublishCmdCommon::parseCommandLineOption(iter))
            return true;

        return false;
    }

    bool finalizeOptions(IProperties *globals)
    {

        if (optESDLService.isEmpty())
            throw MakeStringException( 0, "Name of ESDL service definition must be provided!" );

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

        return EsdlPublishCmdCommon::finalizeOptions(globals);
    }
};

class EsdlListESDLDefCmd : public EsdlPublishCmdCommon
{
public:
    int processCMD()
    {
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientListESDLDefinitionsRequest> req = esdlConfigClient->createListESDLDefinitionsRequest();

        fprintf(stdout,"\nAttempting to list ESDL definitions.\n");

        Owned<IClientListESDLDefinitionsResponse> resp = esdlConfigClient->ListESDLDefinitions(req);

        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        IArrayOf<IConstESDLDefinition> & defs = resp->getDefinitions();

        if (defs.length() > 0)
            fprintf(stdout, "\nESDL Definitions found:\n");

        ForEachItemIn(defindex, defs)
        {
            IConstESDLDefinition & def = defs.item(defindex);
            fprintf(stdout, "\t%s \n", def.getId());
        }
        return 0;
    }

    void usage()
    {
        printf( "\nUsage:\n\n"
                "esdl list-definitions [command options]\n\n"
              );

              EsdlPublishCmdCommon::usage();
    }

    bool parseCommandLineOptions(ArgvIterator &iter)
    {
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
        if (EsdlPublishCmdCommon::parseCommandLineOption(iter))
            return true;

        return false;
    }

    bool finalizeOptions(IProperties *globals)
    {
        return EsdlPublishCmdCommon::finalizeOptions(globals);
    }
};

class EsdlListESDLBindingsCmd : public EsdlPublishCmdCommon
{
public:
    int processCMD()
    {
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientListESDLBindingsRequest> req = esdlConfigClient->createListESDLBindingsRequest();

        fprintf(stdout,"\nAttempting to list ESDL bindings.\n");

        Owned<IClientListESDLBindingsResponse> resp = esdlConfigClient->ListESDLBindings(req);

        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        IArrayOf<IConstESDLBinding> & binds = resp->getBindings();

        if (binds.length() > 0)
            fprintf(stdout, "\nESDL Bindings found:\n");

        ForEachItemIn(bindindex, binds)
        {
            IConstESDLBinding & bind = binds.item(bindindex);
            fprintf(stdout, "\t%s \n", bind.getId());
        }

        return 0;
    }

    void usage()
    {
        printf( "\nUsage:\n\n"
                "esdl list-bindings [command options]\n\n"
              );

        EsdlPublishCmdCommon::usage();
    }

    bool parseCommandLineOptions(ArgvIterator &iter)
    {
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
        if (EsdlPublishCmdCommon::parseCommandLineOption(iter))
            return true;

        return false;
    }

    bool finalizeOptions(IProperties *globals)
    {
        return EsdlPublishCmdCommon::finalizeOptions(globals);
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

        fprintf(stdout,"\nAttempting to configure Method : '%s'.'%s'\n", optService.get(), optMethod.get());
        request->setEspProcName(optTargetESPProcName);
        request->setEspBindingName(optBindingName);
        request->setEsdlServiceName(optService.get());
        VStringBuffer id("%s.%d", optService.get(), (int)optVersion);
        request->setEsdlDefinitionID(id.str());
        request->setConfig(optInput);
        request->setOverwrite(optOverWrite);

        if (optVerbose)
            fprintf(stdout,"\nMethod config: %s\n", optInput.get());

        Owned<IClientConfigureESDLBindingMethodResponse> resp = esdlConfigClient->ConfigureESDLBindingMethod(request);

        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        fprintf(stdout, "\n %s.\n", resp->getStatus().getDescription());

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

        if (optMethod.isEmpty())
            throw MakeStringException( 0, "Name of ESDL based method must be provided" );

        if (optBindingName.isEmpty())
            throw MakeStringException( 0, "Name of ESP binding must be provided" );

        return EsdlPublishCmdCommon::finalizeOptions(globals);
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
                      optId.set(arg);
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
            if (optId.isEmpty())
                throw MakeStringException( 0, "ESDL ID must be provided" );

            return EsdlPublishCmdCommon::finalizeOptions(globals);
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
            fprintf(stdout, "\n%s.\n", resp->getStatus().getDescription());

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
        fprintf(stdout, "\n%s.\n", resp->getStatus().getDescription());

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
