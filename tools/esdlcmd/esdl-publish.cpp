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
#include "esdlcmdutils.hpp"

class EsdlPublishCmdCommon : public EsdlCmdCommon
{
protected:
    StringAttr optSource;
    StringAttr optService;
    StringAttr optWSProcAddress;
    StringAttr optWSProcPort;
    StringAttr optUser;
    StringAttr optPass;
    StringAttr optESDLDefID;
    StringAttr optESDLService;

    StringAttr optTargetESPProcName;
    StringAttr optTargetAddress;
    StringAttr optTargetPort;
    bool       optOverWrite;
    bool       optSSL = false;

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
        //      >------------------------ 80 columns --------------------------------------------<
         printf(
                "   -s, --server <ip>            IP of server running WsESDLConfig service\n"
                "   --port <port>                WsESDLConfig service port\n"
                "   --ssl                        Use secure connection to the server running\n"
                "                                WsESDLConfig service, even when localhost default.\n"
                "   -u, --username <name>        Username for accessing WsESDLConfig service\n"
                "   -pw, --password <pw>         Password for accessing WsESDLConfig service\n"
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
        if (iter.matchFlag(optUser, ESDL_OPT_SERVICE_USER) || iter.matchFlag(optUser, ESDL_OPTION_SERVICE_USER))
            return true;
        if (iter.matchFlag(optPass, ESDL_OPT_SERVICE_PASS) || iter.matchFlag(optPass, ESDL_OPTION_SERVICE_PASS))
            return true;
        if (iter.matchFlag(optOverWrite, ESDL_OPTION_OVERWRITE) )
            return true;
        if (iter.matchFlag(optSSL, ESDL_OPTION_SSL))
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

protected:
    void outputServerException(IException* e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        DBGLOG("Error communicating with the ESDLConfig service at %s://%s:%s : %s", optSSL ? "https" : "http", optWSProcAddress.get(), optWSProcPort.get(), msg.str());
        e->Release();
    }

};

class EsdlPublishCmd : public EsdlPublishCmdCommon
{
public:
    int processCMD()
    {
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optSSL, optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientPublishESDLDefinitionRequest> request = esdlConfigClient->createPublishESDLDefinitionRequest();

        StringBuffer esxml;
        esdlHelper->getServiceESXDL(optSource.get(), optESDLService.get(), esxml, 0, NULL, (DEPFLAG_INCLUDE_TYPES & ~DEPFLAG_INCLUDE_METHOD), optIncludePath.str(), optTraceFlags());
        if (esxml.length()==0)
        {
            fprintf(stderr,"\nESDL Definition for service %s could not be loaded from: %s\n", optESDLService.get(), optSource.get());
            return -1;
        }
        if (optESDLService.length() != 0)
            request->setServiceName(optESDLService.get());

        if (optVerbose)
            fprintf(stdout,"\nESDL Definition: \n%s\n", esxml.str());

        request->setXMLDefinition(esxml.str());
        request->setDeletePrevious(optOverWrite);

        Owned<IClientPublishESDLDefinitionResponse> resp;
        try
        {
            resp.setown(esdlConfigClient->PublishESDLDefinition(request));
        }
        catch (IException* e)
        {
            outputServerException(e);
            return 1;
        }

        bool validateMessages = false;
        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        StringBuffer message;
        if (optESDLService.length() > 0)
            message.setf("ESDL Service: %s: %s", optESDLService.get(), resp->getStatus().getDescription());
        else
            message.set(resp->getStatus().getDescription());

        outputWsStatus(resp->getStatus().getCode(), message.str());

        return 0;
    }

    void usage()
    {
        printf("\nUsage:\n\n"
                "esdl publish <filename.(ecm|esdl|xml)> <servicename> [command options]\n\n"
                "   <filename.(ecm|esdl|xml)>   The ESDL file containing service definition in esdl format (.ecm |.esdl) or in esxdl format (.xml) \n"
                "   <servicename>               The ESDL defined ESP service to publish. Optional.\n"
                "Options (use option flag followed by appropriate value):\n"
                "   --overwrite                 Overwrite the latest version of this ESDL Definition\n"
                ESDLOPT_INCLUDE_PATH_USAGE
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

       //First parameter is required
       //<filename.(ecm|esdl|xml)>       The ESDL file containing service definition\n"
       //Second parameter is now optional but can cause request to fail if not provided
       //<servicename>
       for (int cur = 0; cur < 2 && !iter.done(); cur++)
       {
          const char *arg = iter.query();
          if (*arg != '-')
          {
              switch (cur)
              {
               case 0:
                   optSource.set(arg);
                   break;
               case 1:
                   optESDLService.set(arg);
                   break;
              }
          }
          else
          {
              if (optSource.isEmpty())
              {
                  fprintf(stderr, "\nOption detected before required arguments: %s\n", arg);
                  usage();
                  return false;
              }
              else
              {
                  fprintf(stderr, "\nWarning: Target ESDL Service was not specified, this request will fail if ESDL definition contains multiple \n");
                  break;
              }
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
        StringAttr oneOption;
        if (iter.matchOption(oneOption, ESDLOPT_INCLUDE_PATH) || iter.matchOption(oneOption, ESDLOPT_INCLUDE_PATH_S))
        {
            if(optIncludePath.length() > 0)
                optIncludePath.append(ENVSEPSTR);
            optIncludePath.append(oneOption.get());
            return true;
        }

        if (EsdlPublishCmdCommon::parseCommandLineOption(iter))
            return true;

        return false;
    }

    bool finalizeOptions(IProperties *globals)
    {
        extractEsdlCmdOption(optIncludePath, globals, ESDLOPT_INCLUDE_PATH_ENV, ESDLOPT_INCLUDE_PATH_INI, NULL, NULL);

        if (optSource.isEmpty())
            throw MakeStringException( 0, "Source ESDL definition file (ecm|esdl|xml) must be provided" );

        return EsdlPublishCmdCommon::finalizeOptions(globals);
    }

private:
    StringBuffer optIncludePath;
};

class EsdlBindServiceCmd : public EsdlPublishCmdCommon
{
protected:
    StringAttr optBindingName;
    StringAttr optPortOrName;
    StringAttr optInput;

public:
    int processCMD()
    {
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optSSL, optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientPublishESDLBindingRequest> request = esdlConfigClient->createPublishESDLBindingRequest();

        fprintf(stdout,"\nAttempting to publish ESDL binding on process %s %s %s\n", optTargetESPProcName.get(), optBindingName.length() > 0?"binding":"port", optPortOrName.get());
        request->setEspProcName(optTargetESPProcName);
        if (optBindingName.length() > 0)
            request->setEspBindingName(optBindingName.str());
        if (optTargetPort.length() > 0)
            request->setEspPort(optTargetPort);
        request->setEsdlServiceName(optESDLService);
        request->setEsdlDefinitionID(optESDLDefID);
        request->setConfig(optInput);
        request->setOverwrite(optOverWrite);

        if (optVerbose)
            fprintf(stdout,"\nMethod config: %s\n", optInput.get());

        Owned<IClientPublishESDLBindingResponse> resp;
        try
        {
            resp.setown(esdlConfigClient->PublishESDLBinding(request));
        }
        catch (IException* e)
        {
            outputServerException(e);
            return 1;
        }

        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        outputWsStatus(resp->getStatus().getCode(), resp->getStatus().getDescription());

        return 0;
    }

    void usage()
    {
        printf( "\nUsage:\n\n"
                "esdl bind-service <TargetESPProcessName> <TargetESPBindingPort | TargetESPBindingName> <ESDLDefinitionId> (<ESDLServiceName>) [command options]\n\n"
                "   TargetESPProcessName                             The target ESP Process name\n"
                "   TargetESPBindingPort | TargetESPBindingName      Either target ESP binding port or target ESP binding name\n"
                "   ESDLDefinitionId                                 The Name and version of the ESDL definition to bind to this service (must already be defined in dali.)\n"
                "   ESDLServiceName                                  The Name of the ESDL Service (as defined in the ESDL Definition)"
                "                                                    *Required if ESDL definition contains multiple services\n"

                "\nOptions (use option flag followed by appropriate value):\n"
                "   --config <file|\"xml\">                          Configuration XML for all methods associated with the target Service\n"
                "   --overwrite                                      Overwrite binding if it already exists\n");

                EsdlPublishCmdCommon::usage();

        printf( "\n Use this command to publish ESDL Service based bindings.\n"
                "   To bind an ESDL Service, provide the target ESP process name\n"
                "   (ESP Process which will host the ESP Service as defined in the ESDL Definition.) \n"
                "   It is also necessary to provide either the Port on which this service is configured to run (ESP Binding),\n"
                "   or the name of the service you are binding.\n"
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

        //First 3 parameter's order is fixed.
        //TargetESPProcessName
        //TargetESPBindingPort | TargetESPServiceName
        //ESDLDefinitionId
        //4th param is now optional, absence can cause failure
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
               if (cur < 3)
               {
                   fprintf(stderr, "\nOption detected before required arguments: %s\n", arg);
                   usage();
                   return false;
               }
               else
               {
                   fprintf(stderr, "\nWarning ESDL Service name not provided. Can cause failure if ESDL definition contains multiple services\n");
                   break;
               }
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
            fprintf(stderr, "Warning: ESDL service definition name was not provided. Request will fail if ESDL def contains multiple service defined.");

        if(optTargetESPProcName.isEmpty())
            throw MakeStringException( 0, "Name of Target ESP process must be provided!" );

        if (optPortOrName.isEmpty())
            throw MakeStringException( 0, "Either the target ESP service port of name must be provided!" );
        else
        {
            const char * portorname =  optPortOrName.get();
            isdigit(*portorname) ? optTargetPort.set(portorname) : optBindingName.set(portorname);
        }

        return EsdlPublishCmdCommon::finalizeOptions(globals);
    }
};

class EsdlUnBindServiceCmd : public EsdlPublishCmdCommon
{
protected:
    StringAttr optEspBinding;
    StringAttr optBindingId;

public:
    int processCMD()
    {
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optSSL, optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientDeleteESDLBindingRequest> request = esdlConfigClient->createDeleteESDLBindingRequest();

        fprintf(stdout,"\nAttempting to un-bind ESDL Service: '%s'\n", optBindingId.get());

        request->setId(optBindingId);

        Owned<IClientDeleteESDLRegistryEntryResponse> resp;
        try
        {
            resp.setown(esdlConfigClient->DeleteESDLBinding(request));
        }
        catch (IException* e)
        {
            outputServerException(e);
            return 1;
        }

        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        outputWsStatus(resp->getStatus().getCode(), resp->getStatus().getDescription());

        return 0;
    }

    void usage()
    {
        printf( "\nUsage:\n\n"
                "esdl unbind-service <EsdlBindingId> [command options]\n\n"
                "   EsdlBindingId      Id of the esdl binding\n"
                "\n\nOptions:\n"
              );

        EsdlPublishCmdCommon::usage();

        printf( "\n   Use this command to unbind ESDL service based bindings.\n"
                "   To unbind a given ESDL binding, provide the esdl binding id\n"
                "   Available ESDL bindings to unbind can be found via the '>esdl list-bindings' command."
                );

        printf("\nExample:"
                ">esdl unbind-service myesp.8003.EsdlExample \n"
                );
    }

    bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
        {
            usage();
            return false;
        }

        for (int cur = 0; cur < 1 && !iter.done(); cur++)
        {
           const char *arg = iter.query();
           if (*arg != '-')
           {
               switch (cur)
               {
                case 0:
                    optBindingId.set(arg);
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

        if (optBindingId.isEmpty())
            throw MakeStringException( 0, "Esdl binding id must be provided!" );

        return EsdlPublishCmdCommon::finalizeOptions(globals);
    }
};

class EsdlDeleteESDLDefCmd : public EsdlPublishCmdCommon
{
public:
    int processCMD()
    {
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optSSL, optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientDeleteESDLDefinitionRequest> request = esdlConfigClient->createDeleteESDLDefinitionRequest();

        fprintf(stdout,"\nAttempting to delete ESDL definition: '%s'\n", optESDLDefID.get());

        request->setId(optESDLDefID.get());

        Owned<IClientDeleteESDLRegistryEntryResponse> resp;
        try
        {
            resp.setown(esdlConfigClient->DeleteESDLDefinition(request));
        }
        catch (IException* e)
        {
            outputServerException(e);
            return 1;
        }

        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        outputWsStatus(resp->getStatus().getCode(), resp->getStatus().getDescription());

        return 0;
    }

    void usage()
    {
        printf( "\nUsage:\n\n"
                "esdl delete <ESDLDefinitionID> [command options]\n\n"
                "   ESDLDefinitionID      The ESDL definition id <esdldefname>.<esdldefver>\n");

        EsdlPublishCmdCommon::usage();

        printf( "\n   Use this command to delete an ESDL Service definition.\n"
                "   To delete an ESDL Service definition, provide the ESDL definition id\n"
                );

        printf("\nExample:"
                ">esdl delete myesdldef.5\n"
                );
    }

    bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
        {
            usage();
            return false;
        }

        for (int cur = 0; cur < 1 && !iter.done(); cur++)
        {
           const char *arg = iter.query();
           if (*arg != '-')
           {
               switch (cur)
               {
                case 0:
                    optESDLDefID.set(arg);
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

        if (optESDLDefID.isEmpty())
            throw MakeStringException( 0, "ESDLDefinitionID must be provided!" );

        return EsdlPublishCmdCommon::finalizeOptions(globals);
    }
};

class EsdlListESDLDefCmd : public EsdlPublishCmdCommon
{
public:
    int processCMD()
    {
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optSSL, optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientListESDLDefinitionsRequest> req = esdlConfigClient->createListESDLDefinitionsRequest();

        fprintf(stdout,"\nAttempting to list ESDL definitions.\n");

        Owned<IClientListESDLDefinitionsResponse> resp;
        try
        {
            resp.setown(esdlConfigClient->ListESDLDefinitions(req));
        }
        catch (IException* e)
        {
            outputServerException(e);
            return 1;
        }

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
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optSSL, optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientListESDLBindingsRequest> req = esdlConfigClient->createListESDLBindingsRequest();

        fprintf(stdout,"\nAttempting to list ESDL bindings.\n");


        Owned<IClientListESDLBindingsResponse> resp;

        try
        {
            resp.setown(esdlConfigClient->ListESDLBindings(req));
        }
        catch (IException* e)
        {
            outputServerException(e);
            return 1;
        }

        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        bool found = false;
        IArrayOf<IConstEspProcessStruct> & processes = resp->getEspProcesses();
        ForEachItemIn(procInd, processes)
        {
            IConstEspProcessStruct& process = processes.item(procInd);
            IArrayOf<IConstEspPortStruct>& ports = process.getPorts();
            ForEachItemIn(portInd, ports)
            {
                IConstEspPortStruct& port = ports.item(portInd);
                IArrayOf<IConstESDLBinding>& bindings = port.getBindings();
                ForEachItemIn(bindInd, bindings)
                {
                    IConstESDLBinding& binding = bindings.item(bindInd);
                    if (!found)
                    {
                        found = true;
                        fprintf(stdout, "\nESDL bindings found:\n");
                    }
                    fprintf(stdout, "\t%s\n", binding.getId());
                }
            }
        }
        if (!found)
            fprintf(stdout, "\nNo ESDL binding found.\n");
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
    StringAttr optBindingId;

public:
    int processCMD()
    {
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optSSL, optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientConfigureESDLBindingMethodRequest> request = esdlConfigClient->createConfigureESDLBindingMethodRequest();

        fprintf(stdout,"\nAttempting to configure Method : '%s' for binding '%s'\n", optMethod.get(), optBindingId.get());
        request->setEsdlBindingId(optBindingId.get());
        request->setMethodName(optMethod.get());
        request->setConfig(optInput);
        request->setOverwrite(optOverWrite);

        if (optVerbose)
            fprintf(stdout,"\nMethod config: %s\n", optInput.get());

        Owned<IClientConfigureESDLBindingMethodResponse> resp;
        try
        {
            resp.setown(esdlConfigClient->ConfigureESDLBindingMethod(request));
        }
        catch (IException* e)
        {
            outputServerException(e);
            return 1;
        }

        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        outputWsStatus(resp->getStatus().getCode(), resp->getStatus().getDescription());

        return 0;
    }

    void usage()
    {
        printf( "\nUsage:\n\n"
                "esdl bind-method <TargetESDLBindingID> <TargetMethodName> [command options]\n\n"
                "   TargetESDLBindingID                              The id of the target ESDL binding (must exist in dali)\n"
                "   TargetMethodName                                 The name of the target method (must exist in the service ESDL definition)\n"

                "\nOptions (use option flag followed by appropriate value):\n"
                "   --config <file|xml>                              Configuration XML for all methods associated with the target Service\n"
                "   --overwrite                                      Overwrite binding if it already exists\n");

                EsdlPublishCmdCommon::usage();

        printf( "\n Use this command to publish ESDL Service based bindings.\n"
                "   To bind a ESDL Service, provide the id of the esdl binding and the method name you're going to configure.\n"
                "   Optionally provide configuration information either directly, or via a\n"
                "   configuration file in the following syntax:\n"
                "     <Methods>\n"
                "     \t<Method name=\"myMethod\" url=\"http://10.45.22.1:292/somepath?someparam=value\" user=\"myuser\" password=\"mypass\"/>\n"
                "     \t<Method name=\"myMethod2\" url=\"http://10.45.22.1:292/somepath?someparam=value\" user=\"myuser\" password=\"mypass\"/>\n"
                "     </Methods>\n"
                );

        printf("\nExample:"
                ">esdl bind-method myesp.8003.EsdlExample myMethod --config /myService/methods.xml\n"
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
        for (int cur = 0; cur < 2 && !iter.done(); cur++)
        {
           const char *arg = iter.query();
           if (*arg != '-')
           {
               switch (cur)
               {
                case 0:
                    optBindingId.set(arg);
                    break;
                case 1:
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

        if (optBindingId.isEmpty())
            throw MakeStringException( 0, "ESDLBindingID must be provided!" );

        if (optMethod.isEmpty())
            throw MakeStringException( 0, "Name of ESDL based method must be provided" );

        return EsdlPublishCmdCommon::finalizeOptions(globals);
    }
};

class EsdlUnBindMethodCmd : public EsdlBindMethodCmd
{
public:
    int processCMD()
    {
        int success = -1;
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optSSL, optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientGetESDLBindingRequest> getrequest = esdlConfigClient->createGetESDLBindingRequest();
        if (optVerbose)
            fprintf(stdout,"\nFetching current ESDL binging configuration for (%s)\n", optBindingId.get());
        getrequest->setEsdlBindingId(optBindingId.get());

        Owned<IClientGetESDLBindingResponse> getresp;
        try
        {
            getresp.setown(esdlConfigClient->GetESDLBinding(getrequest));
        }
        catch (IException* e)
        {
            outputServerException(e);
            return 1;
        }

        if (getresp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(getresp->getExceptions());
            return success;
        }

        if (getresp->getStatus().getCode()!=0)
        {
            fprintf(stderr, "\n Failed to retrieve ESDL Binding configuration for %s: %s.\n", optBindingId.get(), getresp->getStatus().getDescription());
            return success;
        }

        const char * currentconfig = getresp->getConfigXML();
        if (currentconfig && *currentconfig)
        {
            Owned<IPropertyTree> currconfigtree = createPTreeFromXMLString(currentconfig);
            if (currconfigtree)
            {
                VStringBuffer xpath("Definition[1]/Methods/Method[@name='%s']", optMethod.get());

                if (currconfigtree->hasProp(xpath.str()))
                {
                    if (currconfigtree->removeProp(xpath.str()))
                    {
                        StringBuffer newconfig;
                        toXML(currconfigtree, newconfig);

                        Owned<IClientPublishESDLBindingRequest> request = esdlConfigClient->createPublishESDLBindingRequest();

                        if (optVerbose)
                            fprintf(stdout,"\nAttempting to remove method '%s' from esdl binding '%s'\n", optMethod.get(), optBindingId.get());

                        request->setEsdlDefinitionID(getresp->getESDLBinding().getDefinition().getId());
                        request->setEsdlServiceName(getresp->getServiceName());
                        request->setEspProcName(getresp->getEspProcName());
                        request->setEspPort(getresp->getEspPort());
                        request->setConfig(newconfig.str());
                        request->setOverwrite(true);

                        Owned<IClientPublishESDLBindingResponse> resp = esdlConfigClient->PublishESDLBinding(request);

                        if (resp->getExceptions().ordinality() > 0)
                        {
                            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
                            return success;
                        }

                        if (resp->getStatus().getCode() == 0)
                        {
                            fprintf(stdout, "\nSuccessfully unbound method %s from ESDL Binding %s.\n", optMethod.get(), optBindingId.get());
                            success = 0;
                        }
                        else
                            fprintf(stderr, "\nCould not unbound method %s from ESDL Binding %s: %s\n", optMethod.get(), optBindingId.get(), resp->getStatus().getDescription());
                    }
                    else
                        fprintf(stderr, "\n Could not remove Method %s from ESDL Binding %s configuration.\n", optMethod.get(), optBindingId.get());
                }
                else
                    fprintf(stderr, "\n Method %s doesn't seem to be associated with ESDL Binding %s.\n", optMethod.get(), optBindingId.get());
            }
            else
                fprintf(stderr, "\n Could not interpret configuration for ESDL Binding %s :  %s.\n", optBindingId.get(), currentconfig );
        }
        else
            fprintf(stderr, "\n Received empty configuration for ESDL Binding %s.\n", optBindingId.get());

        return success;
    }

    void usage()
    {
        printf( "\nUsage:\n\n"
                "esdl unbind-method <ESDLBindingID> <MethodName> [\n\n"
                "   ESDLBindingID                              The id of the esdl binding associated with the target method\n"
                "   MethodName                                 The name of the target method (must exist in the service ESDL definition)\n");

                EsdlPublishCmdCommon::usage();

        printf( "\n   Use this command to unbind a method configuration currently associated with a given ESDL binding.\n"
                "   To unbind a method, provide the target esdl binding id and the name of the method to unbind\n");

        printf("\nExample:"
                ">esdl unbind-method myesp.8003.WsMyService mymethod\n"
                );
    }
    bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
        {
            usage();
            return false;
        }

        //First 4 parameter order is fixed.
        for (int cur = 0; cur < 2 && !iter.done(); cur++)
        {
           const char *arg = iter.query();
           if (*arg != '-')
           {
               switch (cur)
               {
                case 0:
                    optBindingId.set(arg);
                    break;
                case 1:
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
        if(optBindingId.isEmpty())
            throw MakeStringException( 0, "Name of Target ESDL Binding must be provided" );

        if (optMethod.isEmpty())
            throw MakeStringException( 0, "Name of ESDL based method must be provided" );

        return EsdlPublishCmdCommon::finalizeOptions(globals);
    }
};

class EsdlBindLogTransformCmd : public EsdlBindServiceCmd
{
protected:
    StringAttr optLogTransform;
    StringAttr optBindingId;
    bool       optEncoded;

public:
    int processCMD()
    {
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optSSL, optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientConfigureESDLBindingLogTransformRequest> request = esdlConfigClient->createConfigureESDLBindingLogTransformRequest();

        fprintf(stdout,"\nAttempting to configure LogTransform : '%s' for binding '%s'\n", optLogTransform.get(), optBindingId.get());
        request->setEsdlBindingId(optBindingId.get());
        request->setLogTransformName(optLogTransform.get());
        request->setConfig(optInput);
        request->setOverwrite(optOverWrite);
        request->setEncoded(optEncoded);

        if (optVerbose)
            fprintf(stdout,"\nLogTransform config: %s\n", optInput.get());

        Owned<IClientConfigureESDLBindingLogTransformResponse> resp;
        try
        {
            resp.setown(esdlConfigClient->ConfigureESDLBindingLogTransform(request));
        }
        catch (IException* e)
        {
            outputServerException(e);
            return 1;
        }

        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        outputWsStatus(resp->getStatus().getCode(), resp->getStatus().getDescription());

        return 0;
    }

    void usage()
    {
        printf( "\nUsage:\n\n"
                "esdl bind-log-transform <TargetESDLBindingID> [TargetLogTransformName] --config <file|xml> [command options]\n\n"
                "   TargetESDLBindingID                              The id of the target ESDL binding (must exist in dali)\n"
                "   TargetLogTransformName                           The name of the target LogTransform (required when config is xsl transform)\n"
                "   --config <file|xml>                              Configuration XML for all LogTransforms associated with the target Service\n"

                "\nOptions (use option flag followed by appropriate value):\n"
                "   --overwrite                                      Overwrite LogTransform if it already exists\n"
                "   --encoded                                        The LogTransform in config has been encoded.\n");

                EsdlPublishCmdCommon::usage();

        printf( "\n Use this command to publish ESDL Service based bindings.\n"
                "   To bind a ESDL Service, provide the id of the esdl binding and the LogTransform name you're going to configure.\n"
                "   Optionally provide configuration information either directly, or via a\n"
                "   configuration file in the following syntax:\n"
                "     <LogTransforms>\n"
                "     \t<LogTransform name=\"myLogTransform\">escaped transform script</LogTransform>\n"
                "     \t<LogTransform name=\"myLogTransform2\">escaped transform script</LogTransform>\n"
                "     </LogTransforms>\n"
                "   or\n"
                "     one transform script\n"
                );

        printf("\nExample:\n"
                ">esdl bind-log-transform myesp.8003.EsdlExample myLogTransform --config /myService/log-transforms.xml\n"
                "or:\n"
                ">esdl bind-log-transform myesp.8003.EsdlExample myLogTransform --config /myService/log-transform.xsl\n"
                );
    }

    bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
        {
            usage();
            return false;
        }

        //First 2 parameter order is fixed.
        for (int cur = 0; cur < 2 && !iter.done(); cur++)
        {
           const char *arg = iter.query();
           if (*arg != '-')
           {
               switch (cur)
               {
                case 0:
                    optBindingId.set(arg);
                    break;
                case 1:
                    optLogTransform.set(arg);
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

    bool parseCommandLineOption(ArgvIterator &iter)
    {
        if (iter.matchFlag(optEncoded, ESDL_OPTION_ENCODED) )
            return true;

        if (EsdlBindServiceCmd::parseCommandLineOption(iter))
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

        if (optBindingId.isEmpty())
            throw MakeStringException( 0, "ESDLBindingID must be provided!" );

        if (optLogTransform.isEmpty() && (strncmp(optInput.str(), "<LogTransforms>", 15) != 0))
            throw MakeStringException( 0, "Name of ESDL based LogTransform must be provided" );

        return EsdlPublishCmdCommon::finalizeOptions(globals);
    }
};

class EsdlUnBindLogTransformCmd : public EsdlBindLogTransformCmd
{
public:
    int processCMD()
    {
        int success = -1;
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optSSL, optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientGetESDLBindingRequest> getrequest = esdlConfigClient->createGetESDLBindingRequest();
        if (optVerbose)
            fprintf(stdout,"\nFetching current ESDL binding configuration for (%s) to unbind log transform (%s)\n", optBindingId.get(), optLogTransform.get());
        getrequest->setEsdlBindingId(optBindingId.get());

        Owned<IClientGetESDLBindingResponse> getresp;
        try
        {
            getresp.setown(esdlConfigClient->GetESDLBinding(getrequest));
        }
        catch (IException* e)
        {
            outputServerException(e);
            return 1;
        }

        if (getresp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(getresp->getExceptions());
            return success;
        }

        if (getresp->getStatus().getCode()!=0)
        {
            fprintf(stderr, "\n Failed to retrieve ESDL Binding configuration for %s: %s.\n", optBindingId.get(), getresp->getStatus().getDescription());
            return success;
        }

        const char * currentconfig = getresp->getConfigXML();
        if (currentconfig && *currentconfig)
        {
            Owned<IPropertyTree> currconfigtree = createPTreeFromXMLString(currentconfig, ipt_caseInsensitive | ipt_ordered);
            if (currconfigtree)
            {
                VStringBuffer xpath("Definition[1]/LogTransforms/LogTransform[@name='%s']", optLogTransform.get());

                if (currconfigtree->hasProp(xpath.str()))
                {
                    if (currconfigtree->removeProp(xpath.str()))
                    {
                        StringBuffer newconfig;
                        toXML(currconfigtree, newconfig);

                        Owned<IClientPublishESDLBindingRequest> request = esdlConfigClient->createPublishESDLBindingRequest();

                        if (optVerbose)
                            fprintf(stdout,"\nAttempting to remove LogTransform '%s' from esdl binding '%s'\n", optLogTransform.get(), optBindingId.get());

                        request->setEsdlDefinitionID(getresp->getESDLBinding().getDefinition().getId());
                        request->setEsdlServiceName(getresp->getServiceName());
                        request->setEspProcName(getresp->getEspProcName());
                        request->setEspPort(getresp->getEspPort());
                        request->setConfig(newconfig.str());
                        request->setOverwrite(true);

                        Owned<IClientPublishESDLBindingResponse> resp;
                        try
                        {
                            resp.setown(esdlConfigClient->PublishESDLBinding(request));
                        }
                        catch (IException* e)
                        {
                            outputServerException(e);
                            return 1;
                        }

                        if (resp->getExceptions().ordinality() > 0)
                        {
                            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
                            return success;
                        }

                        if (resp->getStatus().getCode() == 0)
                        {
                            fprintf(stdout, "\nSuccessfully unbound LogTransform %s from ESDL Binding %s.\n", optLogTransform.get(), optBindingId.get());
                            success = 0;
                        }
                        else
                            fprintf(stderr, "\nCould not unbound LogTransform %s from ESDL Binding %s: %s\n", optLogTransform.get(), optBindingId.get(), resp->getStatus().getDescription());
                    }
                    else
                        fprintf(stderr, "\n Could not remove LogTransform %s from ESDL Binding %s configuration.\n", optLogTransform.get(), optBindingId.get());
                }
                else
                    fprintf(stderr, "\n LogTransform %s doesn't seem to be associated with ESDL Binding %s.\n", optLogTransform.get(), optBindingId.get());
            }
            else
                fprintf(stderr, "\n Could not interpret configuration for ESDL Binding %s :  %s.\n", optBindingId.get(), currentconfig );
        }
        else
            fprintf(stderr, "\n Received empty configuration for ESDL Binding %s.\n", optBindingId.get());

        return success;
    }

    void usage()
    {
        printf( "\nUsage:\n\n"
                "esdl unbind-log-transform <ESDLBindingID> <LogTransformName> [\n\n"
                "   ESDLBindingID                              The id of the esdl binding associated with the target LogTransform\n"
                "   LogTransformName                           The name of the target LogTransform (must exist in the service ESDL definition)\n");

                EsdlPublishCmdCommon::usage();

        printf( "\n   Use this command to unbind a LogTransform configuration currently associated with a given ESDL binding.\n"
                "   To unbind a LogTransform, provide the target esdl binding id and the name of the LogTransform to unbind\n");

        printf("\nExample:"
                ">esdl unbind-log-transform myesp.8003.WsMyService myLogTransform\n"
                );
    }
    bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
        {
            usage();
            return false;
        }

        //First 2 parameter order is fixed.
        for (int cur = 0; cur < 2 && !iter.done(); cur++)
        {
           const char *arg = iter.query();
           if (*arg != '-')
           {
               switch (cur)
               {
                case 0:
                    optBindingId.set(arg);
                    break;
                case 1:
                    optLogTransform.set(arg);
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
        if(optBindingId.isEmpty())
            throw MakeStringException( 0, "Name of Target ESDL Binding must be provided" );

        if (optLogTransform.isEmpty())
            throw MakeStringException( 0, "Name of ESDL based LogTransform must be provided" );

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
            Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optSSL, optWSProcAddress, optWSProcPort, optUser, optPass);
            Owned<IClientGetESDLDefinitionRequest> request = esdlConfigClient->createGetESDLDefinitionRequest();

            fprintf(stdout,"\nAttempting to get ESDL definition: %s\n", optId.get());

            request->setId(optId);

            Owned<IClientGetESDLDefinitionResponse> resp;
            try
            {
                resp.setown(esdlConfigClient->GetESDLDefinition(request));
            }
            catch (IException* e)
            {
                outputServerException(e);
                return 1;
            }

            if (resp->getExceptions().ordinality()>0)
            {
                EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
                return 1;
            }

            StringBuffer definition;
            definition.set(resp->getXMLDefinition());

            if (definition.length()==0) //as of wsesdlconfig 1.4 getxmldef moves to definition/Interface
                definition.set(resp->getDefinition().getInterface());

            if (definition.length()!=0)
                fprintf(stdout, "\n%s", definition.str());

            outputWsStatus(resp->getStatus().getCode(), resp->getStatus().getDescription());

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
        Owned<IClientWsESDLConfig> esdlConfigClient = EsdlCmdHelper::getWsESDLConfigSoapService(optSSL, optWSProcAddress, optWSProcPort, optUser, optPass);
        Owned<IClientGetESDLBindingRequest> request = esdlConfigClient->createGetESDLBindingRequest();

        fprintf(stdout,"\nAttempting to get ESDL binding: %s\n", optId.get());

        request->setEsdlBindingId(optId);

        Owned<IClientGetESDLBindingResponse> resp;
        try
        {
            resp.setown(esdlConfigClient->GetESDLBinding(request));
        }
        catch (IException* e)
        {
            outputServerException(e);
            return 1;
        }

        if (resp->getExceptions().ordinality()>0)
        {
            EsdlCmdHelper::outputMultiExceptions(resp->getExceptions());
            return 1;
        }

        fprintf(stdout, "\n%s", resp->getConfigXML());
        outputWsStatus(resp->getStatus().getCode(), resp->getStatus().getDescription());

        return 0;
    }

    void usage()
    {

        printf( "\nUsage:\n\n"
                "esdl get-binding <ESDLBindingId> [command options]\n\n"
                "Options (use option flag followed by appropriate value):\n"
                "   ESDLBindingId            The target ESDL binding id\n"
               );

                EsdlPublishCmdCommon::usage();

        printf( "\n\n Use this command to get DESDL Service based bindings.\n");
    }
};
