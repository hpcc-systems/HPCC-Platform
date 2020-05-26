/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

#include "jargv.hpp"
#include "ws_codesign.hpp"
#include "eclcmd_common.hpp"

class EclCmdSign : public EclCmdCommon
{
public:
    EclCmdSign() {}
    virtual ~EclCmdSign() {}
    virtual eclCmdOptionMatchIndicator parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
            return EclCmdOptionNoMatch;

        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (*arg != '-' || streq(arg, "-"))
            {
                if (optInput.length())
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return EclCmdOptionCompletion;
                }
                optInput.set(arg);
                continue;
            }
            if (iter.matchOption(optOutput, ECLOPT_OUTPUT))
                continue;
            if (iter.matchOption(optKeyUid, ECLOPT_KEYUID))
                continue;
            if (iter.matchOption(optKeyPass, ECLOPT_KEYPASS))
                continue;
            if (iter.matchFlag(optOverwrite, ECLOPT_OVERWRITE))
                continue;
            eclCmdOptionMatchIndicator ind = EclCmdCommon::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return EclCmdOptionMatch;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        if (optInput.length() == 0)
        {
            fprintf(stderr, "\nInput <file> must be provided\n");
            return false;
        }
        if (optKeyUid.length() == 0)
        {
            fprintf(stderr, "\nOption %s must be provided\n", ECLOPT_KEYUID);
            return false;
        }
        return true;
    }
    virtual int processCMD()
    {
        if (optOutput.length() > 0 && !checkOutputFile())
            return 1;
        StringBuffer passbuf;
        if (optKeyPass.length() > 0)
        {
            if (streq(optKeyPass.get(), "-"))
                passwordInput("Please enter password for the key: ", passbuf);
            else
                passbuf.append(optKeyPass.get());
        }
        StringBuffer inputbuf;
        if (streq(optInput.get(), "-"))
        {
            Owned<IFile> file = createIFile("stdin:");
            inputbuf.loadFile(file.get());
        }
        else
            inputbuf.loadFile(optInput.get());
        if (inputbuf.length() == 0)
        {
            fprintf(stderr, "Input is empty\n");
            return 1;
        }
        Owned<IClientws_codesign> client = createCmdClientExt(ws_codesign, *this, nullptr);
        Owned<IClientSignRequest> req = client->createSignRequest();
        req->setUserID(optKeyUid.get());
        req->setText(inputbuf.str());
        if (passbuf.length() > 0)
        {
            req->setKeyPass(passbuf.str());
            passbuf.clear();
        }
        Owned<IClientSignResponse> resp = client->Sign(req.get());
        if (!resp)
        {
            fprintf(stderr, "Failed to get response from server\n");
            return 1;
        }
        if (resp->getExceptions().ordinality() > 0)
        {
            outputMultiExceptionsEx(resp->getExceptions());
            return 1;
        }
        if (resp->getRetCode() != 0)
        {
            fprintf(stderr, "Error %d - %s\n", resp->getRetCode(), resp->getErrMsg());
            return 1;
        }
        if (!(resp->getSignedText()) || !*(resp->getSignedText()))
        {
            fprintf(stderr, "Response is empty\n");
            return 1;
        }
        if (optOutput.length() > 0)
            writeToOutput(strlen(resp->getSignedText()), resp->getSignedText());
        else
            fprintf(stdout, "%s", resp->getSignedText());
        return 0;
    }

    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'sign' command signs an ecl file or other text files.\n"
            "\n"
            "ecl sign <file>\n"
            "   <file>                 The path of file to be signed. Use - for stdin\n"
            "Options:\n"
            "   --output=<file>        The path of file to write the result to\n"
            "   --overwrite            Overwrite the file that already exists\n"
            "   --keyuid=<uid>         The user ID of the key for the sign command\n"
            "   --keypass=<password>   The password of the key for the sign command. Use - to enter from stdin\n"
            "                          When both input and keypass use stdin, keypass will be handled first\n",
            stdout);
        EclCmdCommon::usage();
    }

private:
    StringAttr optInput;
    StringAttr optOutput;
    StringAttr optKeyUid;
    StringAttr optKeyPass;
    bool optOverwrite = false;

    bool checkOutputFile()
    {
        Owned<IFile> f = createIFile(optOutput.get());
        if (f->exists())
        {
            if (f->isDirectory() == fileBool::foundYes)
            {
                fprintf(stderr, "Output file specified is a directory\n");
                return false;
            }
            else if (!optOverwrite)
            {
                fprintf(stderr, "File %s already exists, please specify the \"%s\" option for overwriting\n", optOutput.get(), ECLOPT_OVERWRITE);
                return false;
            }
        }
        return true;
    }
    void writeToOutput(size32_t len, const char* content)
    {
        Owned<IFile> f = createIFile(optOutput.get());
        Owned<IFileIO> fio = f->open(IFOcreaterw);
        if (fio)
        {
            fio->write(0, len, (const void*)content);
            fio->close();
        }
    }
};

class EclCmdListKeyUid : public EclCmdCommon
{
public:
    EclCmdListKeyUid() {}
    virtual ~EclCmdListKeyUid() {}
    virtual eclCmdOptionMatchIndicator parseCommandLineOptions(ArgvIterator &iter)
    {
        for (; !iter.done(); iter.next())
        {
            eclCmdOptionMatchIndicator ind = EclCmdCommon::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return EclCmdOptionMatch;
    }
    virtual int processCMD()
    {
        Owned<IClientws_codesign> client = createCmdClientExt(ws_codesign, *this, nullptr);
        Owned<IClientListUserIDsRequest> req = client->createListUserIDsRequest();
        Owned<IClientListUserIDsResponse> resp = client->ListUserIDs(req.get());
        if (!resp)
        {
            fprintf(stderr, "Failed to get response from server\n");
            return 1;
        }
        if (resp->getExceptions().ordinality() > 0)
        {
            outputMultiExceptionsEx(resp->getExceptions());
            return 1;
        }
        StringArray& userids = resp->getUserIDs();
        if (userids.length() > 0)
        {
            StringBuffer uidsbuf;
            ForEachItemIn(i, userids)
                uidsbuf.appendf("%s\n", userids.item(i));
            fprintf(stdout, "%s", uidsbuf.str());
        }
        else
            fprintf(stderr, "No key user IDs found\n");
        return 0;
    }

    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'listkeyuid' command lists all the key user IDs that can be used in the sign command.\n"
            "\n"
            "ecl listkeyuid\n"
            "Options:\n",
            stdout);
        EclCmdCommon::usage();
    }
};

IEclCommand *createSignEclCommand()
{
    return new EclCmdSign();
}
IEclCommand *createListKeyUidCommand()
{
    return new EclCmdListKeyUid();
}
