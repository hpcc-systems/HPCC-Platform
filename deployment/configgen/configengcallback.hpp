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
#if !defined CONFIGENGCALLBACK_HPP
#define CONFIGENGCALLBACK_HPP

#include "deploy.hpp"

class CConfigEngCallback: public CInterface, implements IDeploymentCallback
{
    virtual void printStatus(IDeployTask* task)  
    {
        if (!m_verbose || !task || !task->isProcessed())
            return;

        StringBuffer sb;
        const char* name = task->getCompName();
        if (!name || !*name)
            return;
        
        const char* instance = task->getInstanceName();
        const char* caption = task->getCaption();
        const char* sourceFile = task->getFileSpec(DT_SOURCE);
        const char* targetFile = task->getFileSpec(DT_TARGET);
    
        offset_t size = 0;

        try
        {
            if (!task->getCallback().getAbortStatus())
                size = task->getFileSize(DT_TARGET);
        }
        catch (IException* e)
        {
            e->Release();
        }

        StringBuffer s("0"); 
        if (size>0)
        {
            // format size into: 12,345,456
            s.clear().append(task->getFileSize(DT_TARGET));
            int i = s.length()-3;
            for (;i>0;i-=3)
                s.insert(i,",");
        }

        sb.appendf("\nComponent: %s-%s\nAction: %s\nSource Path:%s\nDestination Path:%s\nTarget Size:%s\n", name, instance, caption, sourceFile, targetFile, s.str());
        
        StringBuffer sbErr(task->getErrorString());
        StringBuffer sbWarnings(task->getWarnings());
        StringBuffer sbErrCode;

        if (task->getErrorCode() > 0)
          sbErrCode.appendlong(task->getErrorCode());
        
        if (sbErr.length() == 0 && sbWarnings.length() == 0 && sbErrCode.length() == 0)
            sb.append("Result: Success\n");
        else
        {
            if (sbErr.length() > 0) sb.appendf("Errors: %s\n", sbErr.str());
            if (sbErrCode.length() > 0) sb.appendf("Error Code: %s\n", sbErrCode.str());
            if (sbWarnings.length() > 0) sb.appendf("Warnings: %s\n", sbWarnings.str());
            sb.append("Result: Errors or warnings raised.\n");
        }

        fprintf(stdout, "%s", sb.str());
    }
    virtual void printStatus(StatusType type, const char* processType, const char* comp, 
        const char* instance, const char* msg=NULL, ...)  __attribute__((format(printf,6,7)))
    {
      if (!m_verbose) return;
      char buf[1024];
            if (msg)
            {
                va_list args;
                va_start(args, msg);
                if (_vsnprintf(buf, sizeof(buf), msg, args) < 0)
                    buf[sizeof(buf) - 1] = '\0';
                va_end(args);
            }
            else
                *buf = '\0';
            
            StringBuffer sb;

            if (processType)
                sb.append("Process type: ").append(processType).append("\n");
            if (comp)
                sb.append("Component: ").append(comp).append("\n");
            if (instance)
                sb.append("Instance: ").append(instance).append("\n");
            if (msg)
                sb.append("Message: ").append(buf).append("\n");
        
        if (STATUS_ERROR == type)
            fprintf(stderr, "%s", sb.str());
        else
            fprintf(stdout, "%s", sb.str());
    }
    virtual bool onDisconnect(const char* target){return true;}
    virtual bool getAbortStatus()const {return m_abort;}
    virtual void setAbortStatus(bool bAbort){m_abort = bAbort;}
    virtual void setEnvironmentUpdated(){}
    virtual void getSshAccountInfo(StringBuffer& userid, StringBuffer& password)const{}
    //the following throws exception on abort, returns true for ignore
    virtual bool processException(const char* processType, const char* process, const char* instance, 
        IException* e, const char* szMessage=NULL, const char* szCaption=NULL,
        IDeployTask* pTask = NULL )
    {
      StringBuffer sb;

      if (m_errIdx > 1)
        sb.append("\n");

      sb.appendf("Error %d:\n", m_errIdx++);

      if (szMessage)
      {
        if (processType)
          sb.append("Component type: ").append(processType).append("\n");
        if (process)
          sb.append("Component Name: ").append(process).append("\n");

        StringBuffer errMsg(szMessage);
        String str(errMsg.trim());

#ifdef USE_XALAN
        if (str.lastIndexOf('[') > 0)
        {
          errMsg.clear();
          String* sub1 = str.substring(str.lastIndexOf('[') + 1, str.length() - 1);

          if (sub1)
          {
            String* sub2 = sub1->substring(sub1->indexOf(':') + 1, sub1->length());

            if (sub2)
            {
              StringBuffer sb2(*sub2);
              sb2.trim();
              sb2.replaceString("  ", " ");
              errMsg.append(sb2.str());
              delete sub2;
            }

            delete sub1;
          }
        }
#endif

        sb.append(errMsg).append("\n");
        m_sbExMsg.append(sb);
      }

      if (m_abortOnException)
      {
        m_abort = true;
        throw MakeStringException(0, "%s", m_sbExMsg.str());
      }

      return true;
    }
    virtual IEnvDeploymentEngine* getEnvDeploymentEngine()const{return NULL;}
    virtual void* getWindowHandle() const{return NULL;}
    virtual void installFileListChanged(){}
    virtual void fileSizeCopied(offset_t size, bool bWholeFileDone){}

private:
    bool m_verbose;
    bool m_abortOnException;
    bool m_abort;
    StringBuffer m_sbExMsg;
    unsigned m_errIdx;
public: // IDeploymentCallback
    IMPLEMENT_IINTERFACE;
    CConfigEngCallback(bool verbose = false, bool abortOnException = false){m_verbose = verbose;m_abortOnException=abortOnException;m_abort=false;m_errIdx=1;}
    virtual const char* getErrorMsg() { return m_sbExMsg.str(); }
    virtual unsigned getErrorCount() { return m_errIdx;}
};
#endif
