/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef RMTSSH_HPP
#define RMTSSH_HPP

#ifdef RMTSSH_LOCAL
#define RMTSSH_API
#else
#ifdef REMOTE_EXPORTS
#define RMTSSH_API DECL_EXPORT
#else
#define RMTSSH_API DECL_IMPORT
#endif
#endif

interface IFRunSSH: extends IInterface
{
    virtual void init(int argc,char * argv[]) = 0;
    virtual void exec() = 0;

    virtual void init(
                const char *cmdline,
                const char *identfilename,
                const char *username,
                const char *passwordenc,
                unsigned timeout,
                unsigned retries) = 0;
    virtual void exec(
              const IpAddress &ip,
              const char *workdirname,
              bool _background) = 0;
    virtual const StringArray &getReplyText() const = 0;
    virtual const UnsignedArray &getReply() const= 0;
};


extern RMTSSH_API IFRunSSH *createFRunSSH();


#endif
