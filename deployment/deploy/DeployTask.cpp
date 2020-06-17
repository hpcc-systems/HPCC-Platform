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
#include <string>
#include "deploy.hpp"
#include "jcrc.hpp"  
#include "jexcept.hpp"
#include "jfile.hpp"
#include "jptree.hpp"  
#include "jstring.hpp"  
#include "jmutex.hpp"
#include "xslprocessor.hpp"

offset_t getDirSize(const char* path, bool subdirs = true)
{
  offset_t size = 0;

  if (!path || !*path)
    return size;

  Owned<IFile> pFile = createIFile(path);

  if (pFile->exists())
  {
    if (pFile->isDirectory() == fileBool::foundYes)
    {
      Owned<IDirectoryIterator> it = pFile->directoryFiles(NULL, false, true);

      ForEach(*it)
      {
        if (it->isDir())
        {
          if (subdirs)
          {
            StringBuffer subdir;
            it->getName(subdir);
            StringBuffer dir(path);
            dir.append(PATHSEPCHAR).append(subdir);
            size += getDirSize(dir.str(), subdirs);
          }
        }
        else
          size += it->getFileSize();
      }
    }
    else
      size = pFile->size();
  }

  return size;
}

class CDeployTask : public CInterface, implements IDeployTask
{
public:
   IMPLEMENT_IINTERFACE;
   //---------------------------------------------------------------------------
   //  CDeployTask
   //---------------------------------------------------------------------------
    CDeployTask(IDeploymentCallback& callback,
               const char* caption, const char* processType, const char* comp, 
               const char* instance, const char* source, const char* target, 
               const char* sshUser, const char* sshKeyFile, const char* sshKeyPassphrase,
               bool useSsh, EnvMachineOS os, const char* processName=NULL)
    : m_caption(caption), m_compName(comp), 
      m_instanceName(instance), m_processed(false), m_errorCode(0), 
      m_abort(false), m_dummy(false), m_machineOS(os), m_flags(0),
      m_processType(processType),
      m_processName(processName),
      m_targetFileWithWrongCase(false),
      m_sshUser(sshUser),
      m_sshKeyFile(sshKeyFile),
      m_sshKeyPassphrase(sshKeyPassphrase),
      m_useSsh(useSsh),
      m_updateProgress(true)
   {
      m_pCallback.set(&callback);
      m_fileSpec[DT_SOURCE].set(source ? source : "");
      m_fileSpec[DT_TARGET].set(target ? target : "");
   }
   //---------------------------------------------------------------------------
   //  getCaption
   //---------------------------------------------------------------------------
   const char* getCaption() const
   {
      return m_caption;
   }
   //---------------------------------------------------------------------------
   //  getCaption
   //---------------------------------------------------------------------------
   void setCaption(const char* caption)
   {
      m_caption.set(caption);
   }
   //---------------------------------------------------------------------------
   //  getCompName
   //---------------------------------------------------------------------------
   const char* getCompName() const
   {
      return m_compName;
   }
   //---------------------------------------------------------------------------
   //  getInstanceName
   //---------------------------------------------------------------------------
   const char* getInstanceName() const
   {
      return m_instanceName;
   }
   //---------------------------------------------------------------------------
   //  getFileSpec
   //---------------------------------------------------------------------------
   const char* getFileSpec(int idx) const
   {
      assertex(idx >= 0 && idx < 2);
      return m_fileSpec[idx];
   }
   //---------------------------------------------------------------------------
   //  getFileName
   //---------------------------------------------------------------------------
   const char* getFileName(int idx) const
   {
      assertex(idx >= 0 && idx < 2);
      return pathTail(m_fileSpec[idx]);
   }
   //---------------------------------------------------------------------------
   //  getFileExt
   //---------------------------------------------------------------------------
   const char* getFileExt(int idx) const
   {
      return findFileExtension(m_fileSpec[idx]);
   }
   //---------------------------------------------------------------------------
   //  getFilePath
   //---------------------------------------------------------------------------
   StringAttr getFilePath(int idx) const
   {
      assertex(idx >= 0 && idx < 2);

      StringBuffer dir;
      splitDirTail(m_fileSpec[idx], dir);

        unsigned int dirLen = dir.length();
        if (dirLen-- && isPathSepChar(dir.charAt(dirLen)))//remove trailing \ or /
            dir.setLength(dirLen);
      return dir.str();
   }
   //---------------------------------------------------------------------------
   //  getFileExists
   //---------------------------------------------------------------------------
   bool getFileExists(int idx) const
   {
      assertex(idx >= 0 && idx < 2);

      if (m_machineOS == MachineOsLinux && m_sshUser.length() && m_sshKeyFile.length())
        return checkSSHFileExists(m_fileSpec[idx]);
      else
      {
        Owned<IFile> pFile = createIFile(m_fileSpec[idx]);
        return pFile->exists();
      }
   }
    //---------------------------------------------------------------------------
    //  getFileSize
    //---------------------------------------------------------------------------
    offset_t getFileSize(int idx) const
    {
        assertex(idx >= 0 && idx < 2);
        Owned<IFile> pFile = createIFile(m_fileSpec[idx]);
        return pFile->size();
    }

  EnvMachineOS getMachineOS() const
  {
    return m_machineOS;
  }

  const char* getSSHUser() const
  {
    return m_sshUser.str();
  }
  
  const char* getSSHKeyFile() const
  {
    return m_sshKeyFile.str();
  }

  const char* getSSHKeyPassphrase() const
  {
    return m_sshKeyPassphrase.str();
  }

  bool checkSSHFileExists(const char* filename) const
  {
    bool flag = false;
    Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Ensure Path", m_processType, m_compName, 
      m_instanceName, filename, NULL, m_sshUser.str(), m_sshKeyFile.str(), m_sshKeyPassphrase.str(),
      true, m_machineOS);

    try
    {
      StringBuffer destpath, destip, cmd, output, tmp, err;
      stripNetAddr(filename, destpath, destip);
      tmp.appendf("%d", msTick());
      cmd.clear().appendf("[ -e %s ] && echo %s", destpath.str(), tmp.str());
      task->execSSHCmd(destip.str(), cmd, output, err);
      flag = strstr(output.str(), tmp.str()) == output.str();
    }
    catch(IException* e)
    {
     throw e;
    }

    return flag;
  }

  void setFileSpec(int idx, const char* file)
  {
    assertex(idx >= 0 && idx < 2);
    m_fileSpec[idx].set(file && *file ? file : "");
  }
    //---------------------------------------------------------------------------
   //  isProcessed
   //---------------------------------------------------------------------------
   bool isProcessed() const
   {
      return m_processed;
   }
   //---------------------------------------------------------------------------
   //  getErrorCode
   //---------------------------------------------------------------------------
   DWORD getErrorCode() const
   { 
      return m_errorCode;
   }
   //---------------------------------------------------------------------------
   //  getErrorString
   //---------------------------------------------------------------------------
   const char* getErrorString() const
   {
      return m_errorString.str();
   }
   //---------------------------------------------------------------------------
   //  getWarnings
   //---------------------------------------------------------------------------
   const char* getWarnings() const
   {
      return m_warnings.str();
   }
   //---------------------------------------------------------------------------
   //  getAbort
   //---------------------------------------------------------------------------
   virtual bool getAbort() const
   {
      return m_abort;
   }
   //---------------------------------------------------------------------------
   //  getFlags
   //---------------------------------------------------------------------------
   unsigned getFlags() const
   { 
      return m_flags;
   }
   //---------------------------------------------------------------------------
   //  setFlags
   //---------------------------------------------------------------------------
   void setFlags(unsigned flags)
   { 
      m_flags = flags;
   }

   void setUpdateProgress(bool flag)
   {
     m_updateProgress = flag;
   }

   //---------------------------------------------------------------------------
   //  createFile
   //---------------------------------------------------------------------------
   bool createFile(const char* text)
   {
      const char* szOpenMode = m_machineOS == MachineOsLinux ? "wb" : "wt";
      m_processed = true;

      assertex(text);
      const char* target = getFileSpec(DT_TARGET);
      while (true)
      {
         m_errorCode = 0;
         m_warnings.clear();
         m_errorString.clear();

         if (m_dummy) break;
         FILE* fp = fopen(target, szOpenMode);
         if (fp)
         {
            DWORD bytesWritten = 0;
            bytesWritten = fwrite(text, 1, strlen(text), fp);
            if (bytesWritten == strlen(text))
            {
               fflush(fp);
               fclose(fp);
               break;
            }
         }

         // Prompt to retry on error
         m_errorCode = getLastError();
         m_errorString.appendf("Cannot create %s: ", target);

         if (fp) fclose(fp); // do this after retrieving last error

         if (showErrorMsg("Error creating file")) //ignore or abort?
            break;
      }
      return (m_errorCode == 0);
   }
   //---------------------------------------------------------------------------
   //  transformFile
   //---------------------------------------------------------------------------
   bool transformFile(IXslProcessor& processor, IXslTransform& transform, const char* tempPath)
   {
     m_processed = true;
     bool bDeleteFile = true;

     const char* xsl = getFileSpec(DT_SOURCE);
     const char* target = getFileSpec(DT_TARGET);
     while (true)
     {
       try
       {
         m_errorCode = 0;
         m_warnings.clear();
         m_errorString.clear();

         const char* processName = m_processName ? m_processName : m_compName;
         transform.setParameter("process", StringBuffer("'").append(processName).append("'").str());
         transform.setParameter("outputFilePath", StringBuffer("'").append(target).append("'").str());
         transform.setParameter("tempPath", StringBuffer("'").append(tempPath).append("'").str());

         if (m_machineOS == MachineOsLinux) 
           transform.setParameter("isLinuxInstance", "1");//set optional parameter

         if (m_instanceName.length())
           transform.setParameter("instance", StringBuffer("'").append(m_instanceName).append("'").str());

         transform.loadXslFromFile(xsl);
         if (!m_dummy)
         {
           if(m_machineOS != MachineOsLinux)
           {
             transform.setResultTarget(target);
             Owned<IFile> pTargetFile = createIFile(target);
             if (pTargetFile->exists() && pTargetFile->isReadOnly()==fileBool::foundYes)
               pTargetFile->setReadOnly(false);
             pTargetFile.clear();

             processor.execute(&transform);
           }
           else
           {
             char tempfile[_MAX_PATH];
             StringBuffer sb;
             StringBuffer sbtmp;
             sbtmp.appendf("%d", msTick());
             bool flag = false;

             {
              Owned<IFile> pTargetFile;

              StringBuffer tmpoutbuf;
              transform.transform(tmpoutbuf);
              tmpoutbuf.replaceString("\r\n", "\n");
             
              if (m_useSsh && !m_sshUser.isEmpty() && !m_sshKeyFile.isEmpty())
              {
                StringBuffer destpath, ip;
                stripNetAddr(target, destpath,ip);
                if (ip.length())
                {
                  getTempPath(tempfile, sizeof(tempfile), m_compName);
                  sb.append(tempfile).append(sbtmp);
                  pTargetFile.setown(createIFile(sb.str()));
                  flag = true;
                }
              }

              if (!flag)
                pTargetFile.setown(createIFile(target));
             
              if (pTargetFile->exists() && pTargetFile->isReadOnly()==fileBool::foundYes)
                pTargetFile->setReadOnly(false);
              Owned<IFileIO> pTargetFileIO = pTargetFile->openShared(IFOcreate, IFSHfull);
              pTargetFileIO->write( 0, tmpoutbuf.length(), tmpoutbuf.str());
             }

             if (flag)
             {
               Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Copy File", 
                 m_processType, m_compName, m_instanceName, sb.str(), target, m_sshUser.str(),
                 m_sshKeyFile.str(), m_sshKeyPassphrase.str(), true, m_machineOS);

               try
               {
                 task->setUpdateProgress(false);
                 task->copyFile(DEFLAGS_CONFIGFILES);
               }
               catch(IException* e)
               {
                 if (!DeleteFile(sb.str()))
                   WARNLOG("Couldn't delete file %s", sb.str());
                 throw e;
               }

               if (!DeleteFile(sb.str()))
                 WARNLOG("Couldn't delete file %s", sb.str());
              }
           }

           m_warnings.set(transform.getMessages());
           transform.closeResultTarget();
#ifdef _WIN32
           //Samba maps the Windows archive, system, and hidden file attributes to the owner, group, and world 
           //execute bits of the file respectively.  So if we are deploying to a Linux box then make the file 
           //executable by setting its archive bit.
           //
           if (m_machineOS == MachineOsLinux && !m_useSsh && m_sshUser.isEmpty() && m_sshKeyFile.isEmpty())
             ::SetFileAttributes(target, FILE_ATTRIBUTE_ARCHIVE);
#else
           //UNIMPLEMENTED;
#endif
         }
         if (m_warnings.length() > 0)
         {
             // do not delete file, warning is not fatal
             formatMessage(target,xsl,m_warnings);
             WARNLOG("%s",m_errorString.str());
         }
         break;
       }
       catch (IException* e)
       {
         StringBuffer warning;
         formatMessage(target,xsl,e->errorMessage(warning),true);
         e->Release();
         //remove incomplete (invalid) output file produced thus far
         if (bDeleteFile && !DeleteFile(target))
           WARNLOG("Couldn't delete file %s", target);
       }
       catch (...)
       {
         m_errorString.appendf("Cannot create %s\nusing XSL transform %s\n\n", target, xsl);
         m_errorString.append("Unspecified XSL error");

         //remove incomplete (invalid) output file produced thus far
         if (!DeleteFile(target))
           WARNLOG("Couldn't delete file %s", target);
       }
       // Prompt to retry on error
       m_errorCode = (DWORD) -1;//don't format m_errorString based on last error
       if (showErrorMsg("Error transforming file"))//ignore or abort?
         break;
     }
     return (m_errorCode == 0);
   }
   //---------------------------------------------------------------------------
   //  messageFormat
   //---------------------------------------------------------------------------
   void formatMessage(const char * target, const char * xsl, StringBuffer & in, bool isError=false)
   {
     StringBuffer message;
     message.set(in.str());
     //for better readability of warning, remove redundant prefix of "[XSLT warning: ", if present
     const char* pattern = "[XSLT warning: ";
     int len = strlen(pattern);
     if (!strncmp(message, pattern, len))
       message.remove(0, len);

     //remove the excessive info about XSLT context when this was thrown
     const char* begin = message.str();
     const char* end   = strstr(begin, ", style tree node:");

     if (end)
       message.setLength(end-begin);

     if (isError)
       m_errorString.appendf("Cannot create %s\nusing XSL transform %s\n\n%s", target, xsl, message.str());
     else
       m_errorString.appendf("In %s : %s", xsl, message.str());
   }
   //---------------------------------------------------------------------------
   //  copyFile
   //---------------------------------------------------------------------------
   bool copyFile(unsigned mode)
   {
     if (m_updateProgress)
       m_pCallback->printStatus(this);

     m_errorCode = 0;

     const char* source = getFileSpec(DT_SOURCE);
     const char* target = getFileSpec(DT_TARGET);

     m_msgBoxOwner = false; //this thread hasn't shown msg box yet

     while (true)
     {
       m_warnings.clear();

       if (m_pCallback->getAbortStatus()) 
         break;

       m_errorCode = 0;
       m_errorString.clear();

       if (m_dummy)
         break;

       unsigned dtcFlags = mode >> 2; //gets DTC_TIME | DTC_SIZE | DTC_CRC
       if (dtcFlags)//deploy only if different checked ??
       {
         if (compareFile(dtcFlags | DTC_DEL_WRONG_CASE) || m_pCallback->getAbortStatus()) //flags specified and files match - ignore
         {
           Owned<IFile> f = createIFile(source);
           m_pCallback->fileSizeCopied(f->size(),true);
           break;
         }
         else if (m_targetFileWithWrongCase)
           break;
       }
       else if (m_machineOS != MachineOsW2K && !m_useSsh && m_sshUser.isEmpty() && m_sshKeyFile.isEmpty() &&
              (targetFileExistsWithWrongCaseSensitivity(true) || m_pCallback->getAbortStatus()))
       {
         break;
       }
       //clear file comparison failure errors generated by compareFile above
       m_processed = false;
       m_errorString.clear();
       m_errorCode = 0;

       Owned<IFile> pSrcFile;
       Owned<IFile> pDstFile;

       bool bCopyRC = false;
       try
       {
         pSrcFile.setown(createIFile(source));
         pDstFile.setown(createIFile(target));

         class CCopyFileProgress : implements ICopyFileProgress
         {
         public:
           CCopyFileProgress(): m_percentDone(0),m_sizeReported(0){}
           CCopyFileProgress(CDeployTask* pTask)
             : m_percentDone(0),m_sizeReported(0)
           {
             m_pTask.set(pTask);
             m_pCallback.set(&pTask->getCallback());
             m_originalCaption.append( pTask->getCaption() );
           }
           virtual CFPmode onProgress(unsigned __int64 sizeDone, unsigned __int64 totalSize)
           {
             CFPmode rc = m_pCallback->getAbortStatus() ? CFPcancel : CFPcontinue;
             StringBuffer newCaption;
             newCaption.append(m_originalCaption);

             if (sizeDone && sizeDone != totalSize)
             {
               unsigned int percentDone = (unsigned int) ((sizeDone*100)/totalSize);
               const unsigned int DELTA = 25;
               const unsigned int rounded = (percentDone / DELTA)*DELTA;
               if (rounded > m_percentDone )
                 m_percentDone = rounded;
               else
                 return rc;

               newCaption.appendf(" [%d%%]", m_percentDone);
             }

             if (strcmp(newCaption.str(), m_pTask->getCaption()))
             {
               m_pTask->setCaption(newCaption.str());
               m_pCallback->printStatus(m_pTask);
             }

             // report size copied
             if (sizeDone==totalSize)
               m_pCallback->fileSizeCopied(sizeDone-m_sizeReported, true);
             else if (sizeDone - m_sizeReported>1024) // 1k
             {
               m_pCallback->fileSizeCopied(sizeDone-m_sizeReported, false);
               m_sizeReported = sizeDone;
             }
             return rc;
           }
           virtual ~CCopyFileProgress()
           {
             if (m_pTask)
               if (0 != strcmp(m_originalCaption.str(), m_pTask->getCaption()))
               {
                 m_pTask->setCaption(m_originalCaption);
                 m_pCallback->printStatus(m_pTask);
               }
           }

           virtual void setDeployTask(CDeployTask* pTask)
           {
             m_pTask.set(pTask);
             m_pCallback.set(&pTask->getCallback());
             m_originalCaption.append( pTask->getCaption() );
           }
         private:
           Owned<CDeployTask> m_pTask;
           Owned<IDeploymentCallback> m_pCallback;
           StringBuffer       m_originalCaption;
           unsigned int       m_percentDone;
           offset_t           m_sizeReported;
         };

         CCopyFileProgress copyProgress;
         if (m_updateProgress)
           copyProgress.setDeployTask(this);

         if (m_useSsh && m_machineOS == MachineOsLinux && !m_sshUser.isEmpty() && !m_sshKeyFile.isEmpty())
         {
           int retcode;
           StringBuffer outbuf, cmdline, errbuf;
           StringBuffer destpath,ip;
           StringBuffer passphr;
           getKeyPassphrase(passphr);
           StringBuffer sb(source);
           bool flag = (sb.charAt(sb.length() - 1) == '*');

           if (flag)
             sb.setLength(sb.length() - 1);

           offset_t dirsize = getDirSize(sb.str());

           if (flag && m_updateProgress)
             copyProgress.onProgress(0, dirsize);
           
           stripNetAddr(target, destpath, ip);
           cmdline.appendf("pscp -p -noagent -q %s -i %s -l %s %s \"%s\" %s:%s", flag?"-r":"",
             m_sshKeyFile.str(), m_sshUser.str(), passphr.str(), source, ip.str(), destpath.str());
           retcode = pipeSSHCmd(cmdline.str(), outbuf, errbuf);

           if (retcode)
           {
             m_errorCode = -1;
             String err(errbuf.str());
             int index = err.indexOf('\n');
             String* perr = err.substring(0, index > 0? index : err.length());
             m_errorString.clear().appendf("%s", perr->str());
             delete perr;

             bCopyRC = false;
           }
           else
           {
             if (flag && m_updateProgress)
               copyProgress.onProgress(dirsize, dirsize);
             else if (m_updateProgress)
               copyProgress.onProgress(pSrcFile->size(), pSrcFile->size());

             Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Chmod", m_processType, m_compName, m_instanceName, 
               destpath.str(), NULL, m_sshUser.str(), m_sshKeyFile.str(), m_sshKeyPassphrase.str(), true, m_machineOS);

             try
             {
               StringBuffer cmd;
               cmd.clear().appendf("chmod -R 755 %s", destpath.str());
               task->execSSHCmd(ip.str(), cmd, outbuf, errbuf);
             }
             catch(IException* e)
             {
              throw e;
             }

             bCopyRC = true;
           }
         }
         else
         {
           ::copyFile(pDstFile, pSrcFile, 0x100000,  &copyProgress);
           bCopyRC = true;
         }
       }
       catch (IException* e)
       {
         e->Release();
         bCopyRC = false;
         m_errorCode = getLastError();
       }
       catch (...)
       {
         bCopyRC = false;
         m_errorCode = getLastError();
       }

       //if copy has failed due to access denied error, then change destination
       //file's attributes to normal (reset read only flag, if set) and retry
       if (m_machineOS != MachineOsLinux && !m_useSsh && m_sshUser.isEmpty() && m_sshKeyFile.isEmpty())
       {
         if (!bCopyRC && (pDstFile.get() != NULL) && pDstFile->exists() && pDstFile->isReadOnly()==fileBool::foundYes/*&& m_machineOS != MachineOsLinux*/)
         {
           try
           {
             //BUG#48891 Handle the exception here to allow the user to retry or ignore. 
             pDstFile->setReadOnly(false);
             continue;
           }
           catch(IException* e)
           {
             e->Release();
           }
         }
       }

       /* This method is invoked from multiple threads concurrently and implements the
       following logic.  There are global retry and abort flags that can be set by
       any thread and are shared among threads to coordinate file copying process.
       The first thread that has an error and grabs the mutex pops up the error
       message box to the user.  If the user selects "abort", the global abort flag is
       set and the thread exits.  Other threads with or without errors check this 
       abort flag and exit if it is set.  If the user selected "ignore", the thread 
       simply exits with error condition and other threads operate unaffected.  However,
       if the user selected "retry" then we don't want other pending threads with error
       conditions to keep popping up error messages asking for abort/retry/ignore.  The
       first thread becomes a "master retry thread" for lack of a better term.  This 
       thread dominates popping of the message boxes and other threads abide by user's 
       wishes for the master retry thread.  The idea is that the user should not be asked
       for every single file copy error since the cause of error in most scenarios could 
       simply be of a global nature, for instance, that the target machine is not powered
       on or the target files are locked since the process is already running and needs
       to be stopped.  An event semaphore is used by master retry thread to wake up all
       other threads with errors, which sense the retry flag and silently retry operation.
       */
       if (!bCopyRC)//file copy failed
       {
         // Prompt to retry on error
         m_errorString.appendf("Cannot copy %s to %s: ", source, target);

         synchronized block(s_monitor);
         if (m_pCallback->getAbortStatus())//has some other thread set the global abort flag?
           break; //go back to beginning of loop where we exit on abort

         enum responseType { r_abort = 1, r_retry, r_ignore };
         responseType rc;

         if (!s_msgBoxActive || m_msgBoxOwner) //no other thread has an active message box
         {
           //block other threads if they encounter error state until user responds
           s_msgBoxActive = true;
           m_msgBoxOwner = true;

           if (showErrorMsg("Error copying file")) //ignore or abort?
           {
             setRetryStatus(false);

             if (m_pCallback->getAbortStatus())
             {
               m_errorCode = (DWORD) -1;
               m_errorString.append("Aborted");
               rc = r_abort;
             }
             else
               rc = r_ignore;
           }
           else
           {
             setRetryStatus(true);
             rc = r_retry;
           }
         }
         else
         {
           //some other thread has an active message box
           //wait for that thread to signal us after it either successfully retries, 
           //or if the user ignores that error or aborts deployment
           //
           s_monitor.wait();

           //respect user's wish to retry all queued failed operations
           rc = m_pCallback->getAbortStatus() ? r_abort : getRetryStatus() ? r_retry : r_ignore;            
         }
         if (r_retry != rc)
           break;
       }
       else
       {
         //copy was successful
         if (m_machineOS != MachineOsLinux && !m_useSsh && m_sshUser.isEmpty() && m_sshKeyFile.isEmpty())
         {
           pDstFile->setReadOnly(false);
#ifdef _WIN32
           //Samba maps the Windows archive, system, and hidden file attributes to the owner, group, and world 
           //execute bits of the file respectively.  So if we are deploying to a Linux box then make the file 
           //executable by setting its archive bit.
           //
           if (m_machineOS == MachineOsLinux)
             ::SetFileAttributes(target, FILE_ATTRIBUTE_ARCHIVE);
#else
           //UNIMPLEMENTED;
#endif
         }
         break;
       }
     }//while


     if (m_msgBoxOwner)//did this thread show the message box in last iteration of this loop?
     {
       synchronized block(s_monitor);
       s_msgBoxActive = false;
       m_msgBoxOwner = false;//up for grabs by other threads
       s_monitor.notifyAll();
     }
     else if (m_pCallback->getAbortStatus())
     {
       m_errorCode = (DWORD) -1;
       m_errorString.append("Aborted");
     }

     m_processed = true;
     return (m_errorCode == 0);
   }
   //---------------------------------------------------------------------------
   //  renameFile
   //---------------------------------------------------------------------------
   bool renameFile()
   {
      m_processed = true;

        //IFile::move fails if you rename "\\ip\path\dir1" to "\\ip\path\dir2"
        //so try to rename to "dir2" if both source and dest have same path prefix
        //
        const char* src = getFileSpec(DT_SOURCE);
        const char* dst = getFileSpec(DT_TARGET);

        StringBuffer dstPath;
        bool linremote=(memcmp(src,"//",2)==0);
        if (linremote || !memcmp(src,"\\\\",2)) // see if remote path
        {
            RemoteFilename rfn;
            rfn.setRemotePath(dst);

            if (rfn.isLocal())
            {
                rfn.getLocalPath(dstPath);

                if (m_machineOS == MachineOsLinux)
                {
                    dstPath.replace('\\', '/');
                    dstPath.replace(':', '$');
                }
                else
                {
                    dstPath.replace('/', '\\');
                    dstPath.replace('$', ':');
                }

                dst = dstPath.str();
            }
        }

      Owned<IFile> pFile = createIFile(src);
      while (true)
      {
         m_errorCode = 0;
         m_warnings.clear();
         m_errorString.clear();

         try 
         {
            if (m_dummy) 
               break;

            pFile->move(dst);
            break;
         }
         catch(IException* e)
         {
            e->Release();
         }
         catch(...)
         {
         }
         // Prompt to retry on error
         m_errorString.clear().appendf("Cannot rename %s to %s: ", src, dst);
         if (showErrorMsg("Error renaming file"))//ignore or abort?
            break;
      }
      return (m_errorCode == 0);
   }
   //---------------------------------------------------------------------------
   //  deleteFile
   //---------------------------------------------------------------------------
   bool deleteFile()
   {
      m_processed = true;

      const char* target = getFileSpec(DT_TARGET);
      m_errorCode = 0;
      m_warnings.clear();
      m_errorString.clear();

      bool rc = m_dummy || DeleteFile(target)==TRUE; 
      m_errorCode = rc ? 0 : (DWORD) -1;
      return rc;
   }

   bool getFileAttributes(IFile* pFile, CDateTime& dt, offset_t& size, unsigned mode)
   {
      bool rc = true;
      try
      {
         if (mode & DTC_SIZE)
            size = pFile->size();

         if (mode & DTC_TIME)
            rc = pFile->getTime(NULL, &dt, NULL);
      }
      catch(IException* e)
      {
         e->Release();
         rc = false;
      }
      catch (...)
      {
         rc = false;
      }

      if (!rc)
      {
         m_errorCode = getLastError();
         m_errorString.clear().appendf("File %s does not exist or cannot be accessed: ", pFile->queryFilename());
         formatSystemError(m_errorString, m_errorCode);
      }
      return rc;
   }

   bool targetFileExistsWithWrongCaseSensitivity(bool bDeleteIfFound)
   {
      const char* target = getFileSpec(DT_TARGET);

        StringBuffer targetPath;
        const char* targetFileName = splitDirTail(target, targetPath);
      bool bLoop;
      bool rc = false;

      do
      {
         bLoop = false;
            Owned<IDirectoryIterator> pDirIter = createDirectoryIterator(  targetPath, targetFileName );
            ForEach(*pDirIter)
            {
                StringBuffer existingFileName;
                pDirIter->getName(existingFileName);
                if (0 != strcmp(targetFileName, existingFileName))
                {
               StringBuffer msg;
                    msg.appendf("File '%s' exists as '%s' and may interfere!", targetFileName, existingFileName.str());

               m_errorString.clear().append(msg);
               m_errorCode = (DWORD)-1;

               if (bDeleteIfFound)
                  msg.append("  Would you like to delete the file and try again?");

                    try 
                    {
                  bool bIgnore = m_pCallback->processException( m_processType.get(), m_compName.get(), m_instanceName.get(), NULL, 
                                                                                   msg.str(), "File name conflict!", this);                     
                        if (bIgnore)
                  {
                     m_processed = true;
                     rc = true; 
                  }
                  else
                  {
                     if (m_pCallback->getAbortStatus())
                     {
                        m_abort = true;
                        rc = true; 
                     }
                     else
                     {
                        if (bDeleteIfFound)
                        {
                           StringBuffer path;
                           path.append(targetPath).append(existingFileName);

                           Owned<IFile> pDstFile = createIFile(path.str());
                           pDstFile->remove();
                        }
                        m_errorString.clear();
                        m_errorCode = 0;
                        bLoop = true;
                        rc = false;
                     }
                  }
                    }
                    catch (IException* e)
                    {
                        e->Release();
                        m_abort = m_pCallback->getAbortStatus();
                    }
                    catch (...)
                    {
                        m_abort = m_pCallback->getAbortStatus();
                    }
               break; //found conflict so don't iterate directory any more
                }
         }//ForEach
      } while (bLoop);

      if (rc)
         m_targetFileWithWrongCase = true;

      return rc;
   }

   //---------------------------------------------------------------------------
   //  compareFile
   //---------------------------------------------------------------------------
   bool compareFile(unsigned mode)
   {
      m_errorCode = 0;
      m_warnings.clear();

      if (m_dummy) 
      {
         m_processed = true;
         return true;
      }

      const char* source = getFileSpec(DT_SOURCE);
      const char* target = getFileSpec(DT_TARGET);

      if (m_machineOS != MachineOsW2K)
      {
        if (m_useSsh && !m_sshUser.isEmpty() && !m_sshKeyFile.isEmpty())
        {
          char digestStr[33];
          char* pStr = getMD5Checksum(source, digestStr);
          int retcode;
          StringBuffer outbuf, cmdline, errbuf;
          StringBuffer destpath,ip;
          stripNetAddr(target, destpath, ip);
          StringBuffer passphr;
          getKeyPassphrase(passphr);
          cmdline.appendf("plink -i %s -l %s %s %s md5sum %s", m_sshKeyFile.str(), m_sshUser.str(), passphr.str(), ip.str(), destpath.str());
          retcode = pipeSSHCmd(cmdline, outbuf, errbuf);
          m_processed = true;

          if (retcode == 0)
          {
            if (strncmp(outbuf.str(), pStr, 32))
            {
              m_errorCode = (DWORD) -1;
              m_errorString.clear().append("md5sum values do not compare");
              return false;
            }

            m_errorCode = 0;
            return true;
          }
          else
          {
            m_errorCode = (DWORD) -1;
            String err(errbuf.str());
            int index = err.indexOf('\n');
            String* perr = err.substring(0, index > 0? index : err.length());
            m_errorString.clear().appendf("%s", perr->str());
            delete perr;
            return false;
          }
        }

         const bool bDeleteIfFound = (mode & DTC_DEL_WRONG_CASE) != 0;
         if (!m_useSsh && targetFileExistsWithWrongCaseSensitivity(bDeleteIfFound))
            return false;
          }

      Owned<IFile> pSrcFile = createIFile(source);
      Owned<IFile> pDstFile = createIFile(target);
      CDateTime dtSrc;
      CDateTime dtDst;
      offset_t  szSrc = 0;
      offset_t  szDst = 0;

      if (!getFileAttributes(pSrcFile, dtSrc, szSrc, mode) ||
          !getFileAttributes(pDstFile, dtDst, szDst, mode))
      {
         m_processed = true;
         return false;
      }

      // Compare size
      if (mode & DTC_SIZE)
      {
         if (szSrc != szDst)
         {
            m_errorCode = (DWORD) -1;
            m_errorString.clear().append("File sizes do not compare");
            m_processed = true;
            return false;
         }
      }
      // Compare timestamp
      if (mode & DTC_TIME)
      {
         if (dtSrc.compare(dtDst))
         {
            m_errorCode = (DWORD) -1;
            m_errorString.clear().append("File timestamps do not compare");
            m_processed = true;
            return false;
         }
      }
      // Compare CRC
      if (mode & DTC_CRC)
      {
         unsigned x = getFileCRC(source);
         unsigned y = getFileCRC(target);
         if (x != y)
         {
            m_errorCode = (DWORD) -1;
            m_errorString.clear().append("File CRC values do not compare");
            m_processed = true;
            return false;
         }
      }
      // Everything checked out
      m_processed = true;
      return true;
   }

   bool createDirectoryRecursive(const char* path)
   {
        StringBuffer machine;
        StringBuffer localpath;
        StringBuffer tail;
        StringBuffer ext;

        if (splitUNCFilename(path, &machine, &localpath, &tail, &ext))
        {
            if (machine.length() && localpath.length())
            {
                int len = localpath.length();

                if (len && isPathSepChar(localpath[--len]))
                    localpath.setLength(len);

                if (len == 0)
                    return true;

                StringBuffer path2;
                path2.append(machine).append(localpath);

                if (createDirectoryRecursive(path2))
                {
                    if (m_useSsh && m_machineOS == MachineOsLinux && !m_sshUser.isEmpty() && !m_sshKeyFile.isEmpty())
                        {
                            int retcode;
                            Owned<IPipeProcess> pipe = createPipeProcess();
                            StringBuffer outbuf, cmdline, errbuf;
                            StringBuffer destpath,ip;
                            stripNetAddr(path, destpath, ip);
                            StringBuffer passphr;
                            getKeyPassphrase(passphr);
                            cmdline.appendf("plink -i %s -l %s %s %s %s %s", m_sshKeyFile.str(),
                              m_sshUser.str(), passphr.str(), ip.str(), "mkdir -p", destpath.str());
                            retcode = pipeSSHCmd(cmdline.str(), outbuf, errbuf);

                            if (retcode && retcode != 1)
                            {
                                m_errorCode = retcode;
                                return false;
                            }
                            else
                                return true;
                        }
                    else
                    {
                        // Check if directory already exists
                        Owned<IFile> pFile = createIFile(path);
                        if (pFile->exists())
                        {
                            if (pFile->isDirectory()!=fileBool::foundYes)
                                throw MakeStringException(-1, "%s exists and is not a directory!", path);
                        }
                        else
                            pFile->createDirectory();//throws
                        return true;
                    }
                }
            }
            return false;
        }
        else
            return recursiveCreateDirectory(path);
     }
   //---------------------------------------------------------------------------
   //  createDirectory
   //---------------------------------------------------------------------------
   bool createDirectory()
   {
      m_processed = true;
      m_errorCode = 0;
      
      const char* target = getFileSpec(DT_TARGET);
      while (true)
      {
         m_errorCode = 0;
         m_warnings.clear();
         m_errorString.clear();

         if (m_dummy) 
                break;

            StringBuffer path(target);
            int len = path.length();

            if (len && isPathSepChar(path[--len]))
                path.setLength(len);

         if (createDirectoryRecursive(path.str()))
                break;

         // Prompt to retry on error
         m_errorString.clear().appendf("Cannot create directory %s: ", path.str());
         if (showErrorMsg("Error creating directory"))//ignore or abort?
            break;
      }
      return (m_errorCode == 0);
   }

   void copyRecursive(IFile& src, IFile& dest) //can throw IException
   {
      const char* srcPath = src.queryFilename();
      const char* dstPath = dest.queryFilename();

      while (true)
      {
         try
         {
                if (m_pCallback->getAbortStatus()) 
                {
                    m_errorCode = (DWORD) -1;
                    m_errorString.append("Aborted");
                    break;
                }

            m_errorCode = 0;
            m_errorString.clear();

            if (m_dummy) 
               break;

            if (src.isDirectory()==fileBool::foundYes)
            {
               if (!dest.exists() && !dest.createDirectory())
                  throw MakeStringException(-1, "Failed to create directory %s", dest.queryFilename());

               Owned<IDirectoryIterator> iSrcDirEntry = src.directoryFiles();
               ForEach(*iSrcDirEntry)
               {
                  IFile& srcDirEntry = iSrcDirEntry->query();
                  const char* dirEntryName = pathTail(srcDirEntry.queryFilename());

                  StringBuffer path;
                  path.append(dstPath).append(PATHSEPCHAR).append(dirEntryName);

                  Owned<IFile> destDirEntry = createIFile(path.str());
                  copyRecursive(srcDirEntry, *destDirEntry);
               }
            }
            else
               ::copyFile(&dest, &src);
            break;
         }
         catch(IException* e)
         {
            e->Release();
         }
         catch(...)
         {
         }

         m_errorString.appendf("Cannot copy %s to %s: ", srcPath, dstPath);
         // Prompt to retry on error
         if (showErrorMsg("Error copying file/directory"))//ignore or abort?
            break;

      }//while
   }

   //---------------------------------------------------------------------------
   //  copyDirectory
   //---------------------------------------------------------------------------
   bool copyDirectory()
   {
      m_processed = true;
      m_warnings.clear();

      Owned<IFile> pSrcFile = createIFile(getFileSpec(DT_SOURCE));
      Owned<IFile> pDstFile = createIFile(getFileSpec(DT_TARGET));
      copyRecursive(*pSrcFile, *pDstFile);
      
      return (m_errorCode == 0);
   }
   //---------------------------------------------------------------------------
   //  createProcess
   //---------------------------------------------------------------------------
   bool createProcess(bool wait, const char* user, const char* pwd)
   {

      m_processed = true;
      m_errorCode = 0;
#ifdef _WINDOWS
      const char* target = getFileSpec(DT_TARGET);
      std::string cmdLine = target;
      std::string displayLine;

      // Make sure target file exists
      char cmdPath[_MAX_PATH];
      strcpy(cmdPath, target);
      char* pchSpace = strchr(cmdPath, ' ');// find command line parameters
      if (pchSpace)
         *pchSpace = '\0';// remove command line parameters

      if (m_machineOS == MachineOsLinux) 
      {
         const char* extension = findFileExtension(cmdPath);
         if (extension && (!stricmp(extension, ".bat") || !stricmp(extension, ".exe")))
            cmdPath[extension-cmdPath] = '\0';//rename programs like starup.bat to startup
      }

      Owned<IFile> pFile = createIFile(cmdPath);
      while (!pFile->exists())
      {
         // Prompt to retry
         m_errorCode = (DWORD) -1; //don't format m_errorString based on last error
         m_warnings.clear();
         m_errorString.clear().appendf("File not found: '%s'", cmdPath);

         if (showErrorMsg("Error creating process"))//ignore or abort?
            return false;
      }

        char modulePath[_MAX_PATH+1] = "";

        //find out where configenv is running from and remove file name 'Configenv.exe'
#ifdef _WIN32
      if (GetModuleFileName(NULL, modulePath, _MAX_PATH))
      {
         char* pch = strrchr(modulePath, '\\');
         if (pch)
            *++pch = '\0';
      }
#endif 
      // Determine if process is remote or local
      if (strlen(cmdPath)>2 && cmdPath[0]=='\\' && cmdPath[1]=='\\')
      {  
         // Parse target into computer, dir, and cmd
         char computer[_MAX_PATH];
         strcpy(computer, cmdPath+2);

         char* pchSlash = strchr(computer, '\\');
         assert(pchSlash);

         char* dir = pchSlash+1;
         char* cmd = (char*)pathTail(dir);
         assertex(computer && (computer < dir) && (dir < cmd));
         *(dir-1) = '\0';
         *(cmd-1) = '\0';

         std::string sUser = user;
         StringBuffer sPswd(pwd);

         if (m_machineOS != MachineOsLinux)
         {
            // Replace '$' with ':' in dir part
            char* x = dir;
            while (x = strchr(x, '$')) *x++ = ':';
         }
      
         // Use psexec as default remote control program
#ifdef _WIN32
         if (m_machineOS != MachineOsLinux)
         {
            if (!checkFileExists(".\\psexec.exe"))
               throw MakeStringException(-1, "Configenv cannot find psexec.exe to execute the remote program!");

                cmdLine = modulePath;
                cmdLine.append("psexec.exe \\\\%computer -u %user -p %pwd -i %dir\\%cmd %dir");
         }
         else
         {
            if (!checkFileExists(".\\plink.exe"))
               throw MakeStringException(-1, "Configenv cannot find plink.exe to execute the remote program!");

            sUser = pathTail(user); //if user name is domain\user1 then just get user1

            //replace all '\\' by '/'
            char* x = dir;
            while (x = strchr(x, '\\')) 
               *x++ = '/';

            /* note that if we use plink (cmd line ssh client) for the first time with a computer,
               it generates the following message:

               The server's host key is not cached in the registry. You have no guarantee that the 
               server is the computer you think it is.  The server's key fingerprint is:
               1024 aa:bb:cc:dd:ee:ff:gg:hh:ii:jj:kk:ll:mm:nn:oo:pp
               If you trust this host, enter "y" to add the key to
               PuTTY's cache and carry on connecting.  If you want to carry on connecting just once, 
               without adding the key to the cache, enter "n".If you do not trust this host, press 
               Return to abandon the connection.

               To get around this, we pipe "n" to plink without using its -batch parameter (since
               that simply aborts the connection attempt).  We need help from cmd.exe to do this though...
            */

            /* fix for bug# 6590: Error when trying to start/stop components from configenv 

               if the command being invoked is using cmd.exe and if the configenv has been invoked
               with a UNC path (like \\machine\dir1\...\configenv.exe) then this would fail since
               cmd.exe does not like to invoke commands with UNC names.
               So we cannot use cmd.exe with UNC paths.  Redirecting input file with 'n' does not
               work either.  So we cannot inhibit the prompt as shown above in this case and the 
               user must enter 'y' or 'n' when asked to cache the key.   However, hitting enter
               aborts the connection with return code 0 with the result that the user is notified 
               that command succeeded since plink returns success even though it aborted the connection.

               Find out how configenv was invoked and use the following commands respectively:
               if UNC   : cmd /c \"echo y | .\\plink.exe -ssh -l %user -pw %pwd %computer /%dir/%cmd /%dir\"
               otherwise:                   .\\plink.exe -ssh -l %user -pw %pwd %computer /%dir/%cmd /%dir

             */

            cmdLine.erase();
            cmdLine = "cmd /c \"echo y | \"";
            cmdLine.append(modulePath).append("plink\" -ssh -l %user -pw %pwd %computer sudo bash -c '/%dir/%cmd /%dir'\"");

            StringBuffer sshUserid;
            m_pCallback->getSshAccountInfo(sshUserid, sPswd);
            sUser = sshUserid.str();
         }//linux
#else
         //TODO
#endif

         // Replace known tokens except the password
         const char* const tokens[] = { "%component", "%computer", "%user", "%dir", "%cmd" };
         const char* values[] = { m_compName, computer, sUser.c_str(), dir, cmd };
         const int count = sizeof(tokens) / sizeof(char*);
         std::string::size_type pos;

         for (int i = 0; i < count; i++)
         {
            while ((pos = cmdLine.find(tokens[i])) != std::string::npos)
               cmdLine.replace(pos, strlen(tokens[i]), values[i] ? values[i] : "");
         }


            //replace %pwd by pwd in cmdLine but by <password> in displayLine
            displayLine = cmdLine;
         if ((pos = cmdLine.find("%pwd")) != std::string::npos)
            cmdLine.replace(pos, strlen("%pwd"), sPswd.str() ? sPswd.str() : "" );

         if ((pos = displayLine.find("%pwd")) != std::string::npos)
            displayLine.replace(pos, strlen("%pwd"), "<password>");
      }

      bool rc = false;
      const bool bCD = modulePath[0] == '\\' && modulePath[1] == '\\';
      try 
      {
         if (bCD)
         {
            char winDir[_MAX_PATH+1];
            GetWindowsDirectory(winDir, _MAX_PATH);
            _chdir(winDir);
         }

         rc = createProcess(cmdLine.c_str(), displayLine.c_str(), wait, m_machineOS == MachineOsLinux);
      }
      catch (IException* e)
      {
         if (bCD)
            _chdir(modulePath);
         throw e;
      }
      catch (...)
      {
         if (bCD)
            _chdir(modulePath);
         throw MakeStringException(-1, "Invalid exception!");
      }

      if (bCD)
         _chdir(modulePath);
      return rc;
#else 
    return true;
#endif
   }

   //---------------------------------------------------------------------------
   //  createProcess
   //---------------------------------------------------------------------------
   bool createProcess(const char* cmdLine, const char* displayLine, bool wait, bool captureOutput)
   {
#ifdef _WINDOWS
        char tempfile[_MAX_PATH];
        const char* processName = m_processName ? m_processName : m_compName;
        getTempPath(tempfile, sizeof(tempfile), processName);
        strcat(tempfile, "rexec");

        // Make sure file name is unique - at least during this session
        Owned<IEnvDeploymentEngine> pEnvDepEngine = m_pCallback->getEnvDeploymentEngine();
        sprintf(&tempfile[strlen(tempfile)], "%d.out", pEnvDepEngine->incrementTempFileCount());

      while (true)
      {
         m_errorCode = 0;
         m_warnings.clear();
         m_errorString.clear();

         // Launch the process
         if (m_dummy) 
            break;

            DeleteFile( tempfile );
            if (invoke_program(cmdLine, m_errorCode, wait, captureOutput ? tempfile : NULL))
            {
                if (m_errorCode)
                {
                    m_errorString.appendf("Process '%s' returned exit code of %d: ", displayLine, m_errorCode);
                    m_errorCode = -1; //so showErrorMsg() does not reformat m_errorString
                }

                StringBuffer& outputStr = m_errorCode ? m_errorString : m_warnings;
                Owned<IFile> pFile = createIFile( tempfile );
                if (pFile->exists())
                {
                    Owned<IFileIO> pFileIO = pFile->open(IFOread);
                    offset_t sz = pFile->size();
                    if (sz)
                    {
                        if (m_errorCode)
                            m_errorString.append("\n\n");

                        unsigned int len = outputStr.length();
                        outputStr.ensureCapacity(len+sz);
                        if (sz == pFileIO->read(0, sz, (void*)(outputStr.str() + len)))
                            outputStr.setLength(sz+len);
                    }
                }
                DeleteFile( tempfile );
            }
         else
            m_errorString.appendf("Cannot create process '%s': ", displayLine);

         if (!m_errorString.length() || showErrorMsg("Error executing process"))//ignore or abort?
            break;
      }
      return (m_errorCode == 0);
#else 
    return true;
#endif
   }

   virtual bool execSSHCmd(const char* ip, const char* cmd, StringBuffer& output, StringBuffer& errmsg)
   {
     if (!ip || !*ip || !cmd || !*cmd || m_sshUser.isEmpty() || m_sshKeyFile.isEmpty())
     {
       errmsg.append("Invalid SSH params");
       return false;
     }

     int retcode;
     Owned<IPipeProcess> pipe = createPipeProcess();
     StringBuffer cmdline;
     StringBuffer passphr;
     getKeyPassphrase(passphr);
     cmdline.appendf("plink -i %s -l %s %s %s %s", m_sshKeyFile.str(), m_sshUser.str(), passphr.str(), ip, cmd);
     retcode = pipeSSHCmd(cmdline.str(), output, errmsg);
     m_processed = true;

     if (retcode && retcode != 1)
     {
       m_errorCode = retcode;
       String err(errmsg.str());
       int index = err.indexOf('\n');
       String* perr = err.substring(0, index > 0? index : err.length());
       m_errorString.clear().appendf("%s", perr->str());
       delete perr;
       return false;
     }
     else
       return true;
   }

   int pipeSSHCmd(const char* cmdline, StringBuffer& outbuf, StringBuffer& errbuf)
   {
     int retcode = 1;
     Owned<IPipeProcess> pipe = createPipeProcess();
     StringBuffer workdir("");

     if (pipe->run("ConfigEnv", cmdline, workdir, FALSE, true, true))
     {
       byte buf[4096*2];
       size32_t read = pipe->read(sizeof(buf), buf);
       outbuf.append(read,(const char *)buf);

       if (strstr(outbuf.str(), "Passphrase for key "))
       {
         retcode = 5;
         errbuf.clear().append("Invalid or missing key passphrase. ");
         pipe->abort();
         return retcode;
       }

       retcode = pipe->wait();
       read = pipe->readError(sizeof(buf), buf);
       errbuf.append(read,(const char *)buf);

       if (strstr(errbuf.str(), "Wrong passphrase"))
       {
         retcode = -1;
         errbuf.clear().append("Invalid or missing key passphrase");
       }
       else if (strstr(errbuf.str(), "The server's host key is not cached in the registry."))
       {
         Owned<IPipeProcess> pipe1 = createPipeProcess();
         StringBuffer cmd;
         cmd.appendf("cmd /c \" echo y | %s \"", cmdline);

         if (pipe1->run("ConfigEnv",cmd.str(),workdir,FALSE,true,true))
         {
           size32_t read = pipe1->read(sizeof(buf), buf);
           outbuf.append(read,(const char *)buf);
           retcode = pipe1->wait();
           read = pipe->readError(sizeof(buf), buf);

           if (read)
           {
             errbuf.append(" ").append(read,(const char *)buf);
             retcode = 7;
           }
           else
             errbuf.clear();
           }
         }
         else if (errbuf.length())
         {
           const char* psz = strstr(errbuf.str(), "Authenticating with public key \"");

           if (!psz || psz != errbuf.str())
             retcode = 2;
           else
           {
             const char* psz1 = psz + strlen("Authenticating with public key \"");
             const char* psz2 = strstr(psz1, "\"\r\n");
             psz1 = psz2 + strlen("\"\r\n");

             if (strlen(psz1))
               retcode = 2;
           }
         }
     }

     return retcode;
   }

   void DisconnectNetworkConnection(const char* remoteNameOrIp)
   {
#ifdef _WINDOWS
      IpAddress ip;
      if (!ip.ipset(remoteNameOrIp))
         throw MakeStringException(-1, "Cannot resolve %s", remoteNameOrIp);

       StringBuffer remoteIP;
       ip.getIpText(remoteIP);


      HANDLE hEnum;
      DWORD dwResult = WNetOpenEnum( RESOURCE_CONNECTED, RESOURCETYPE_ANY, 0, NULL, &hEnum );

      if (dwResult != NO_ERROR)
         throw MakeStringException(-1, "Cannot enumerate existing network connections!" );
      else
      {
         do
         {
            DWORD cbBuffer = 16384;
            LPNETRESOURCE lpnrDrv = (LPNETRESOURCE) GlobalAlloc( GPTR, cbBuffer );


            DWORD cEntries = 0xFFFFFFFF;
            dwResult = WNetEnumResource( hEnum, &cEntries, lpnrDrv, &cbBuffer);

            if (dwResult == NO_ERROR)
            {
               for(DWORD i = 0; i < cEntries; i++ )
               {
                  char nameOrIp[MAX_PATH];
                  strcpy(nameOrIp, lpnrDrv[i].lpRemoteName+2);

                  char* pch = strchr(nameOrIp, '\\');
                  if (pch)
                     *pch = '\0';

                  if (!ip.ipset(nameOrIp))
                  {
                     GlobalFree( (HGLOBAL) lpnrDrv );
                     WNetCloseEnum(hEnum);
                     throw MakeStringException(-1, "Cannot resolve host %s", nameOrIp);
                  }

                   StringBuffer ipAddr;
                   ip.getIpText(ipAddr);

                  if (!stricmp(remoteIP.str(), ipAddr.str()))
                  {
                     //we are already connected to this network resource with another user id
                     //so disconnect from it...and attempt to reconnect
                     //
                     m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, "Disconnecting from %s...", lpnrDrv[i].lpRemoteName);
                     m_pCallback->printStatus(this);


                     dwResult = ::WNetCancelConnection2(lpnrDrv[i].lpRemoteName, CONNECT_UPDATE_PROFILE, FALSE);
                     if (dwResult != NO_ERROR)
                     {
                        m_errorCode = dwResult;
                        m_errorString.appendf("Error disconnecting from %s: ", lpnrDrv[i].lpRemoteName);
                        formatSystemError(m_errorString, m_errorCode);
                        throw MakeStringException(-1, m_errorString.str());
                     }

                     m_pCallback->printStatus(this);
                     m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL);
                     dwResult = ERROR_NO_MORE_ITEMS; //to break out of outer loop
                     break;//only disconnect from the first one since this method will be invoked again if more exist
                  }
               }//for
            }
            else 
               if( dwResult != ERROR_NO_MORE_ITEMS )
               {
                  GlobalFree( (HGLOBAL) lpnrDrv );
                  WNetCloseEnum(hEnum);
                  throw MakeStringException(-1, "Cannot complete enumeration for connections to \\%s", remoteNameOrIp);
               }
            GlobalFree( (HGLOBAL) lpnrDrv );

         } while( dwResult != ERROR_NO_MORE_ITEMS );

         WNetCloseEnum(hEnum);
      }
#endif
   } 

   //---------------------------------------------------------------------------
   //  connectTarget
   //---------------------------------------------------------------------------
   bool connectTarget(const char* user, const char* pwd, bool bInteractiveMode/*=true*/)
   {
      m_processed = true;
#if defined( _WIN32) && !defined (_DEBUG)
      if (getenv("LN_CFG_SkipConnectTarget") != NULL)
      {
        m_errorCode = 0;
        m_errorString.clear();
        m_warnings.clear();
        return (m_errorCode == 0);
      }


      NETRESOURCE netRes;
       memset(&netRes, 0, sizeof(netRes));
       netRes.dwType = RESOURCETYPE_DISK;

        StringBuffer uncPath(getFileSpec(DT_TARGET));
        unsigned int uncPathLen = uncPath.length();
        if (uncPathLen-- && isPathSepChar(uncPath.charAt(uncPathLen)))//remove trailing \ or /
            uncPath.setLength(uncPathLen);

       netRes.lpRemoteName = (char*) uncPath.str();

      while (true)
      {
         m_errorCode = 0;
         m_errorString.clear();
         m_warnings.clear();

         if (m_dummy) return true;
         HWND hCallback = (HWND) m_pCallback->getWindowHandle();
         DWORD errCode = ::WNetAddConnection3(hCallback, &netRes, pwd, user, bInteractiveMode ? CONNECT_INTERACTIVE : 0);
         if (errCode != NO_ERROR)
         {
            if (errCode == ERROR_SESSION_CREDENTIAL_CONFLICT)
            {
               char hostNameOrIp[MAX_PATH];
               strcpy(hostNameOrIp, netRes.lpRemoteName+2);

               char* pch = strchr(hostNameOrIp, '\\');
               if (pch)
                  *pch = '\0';

               try {
                  DisconnectNetworkConnection(hostNameOrIp);
               }
               catch (IException* e) {
                  e->Release();
                  m_errorCode = (DWORD) -1;
               }
               catch (...) {
                  m_errorCode = (DWORD) -1;
               }

               if (m_errorCode)
               {
                  StringBuffer caption;
                  caption.appendf("Error connecting to %s: ", netRes.lpRemoteName);

                  m_errorString.clear().append("The target machine is already connected as a different user name.\n"
                                              "Please disconnect any mapped network drive or active sessions.");
                  if (showErrorMsg(caption.str()))//ignore or abort?
                     break;
               }

               if (m_pCallback->getAbortStatus()) 
               {
                  m_errorCode = (DWORD) -1;
                  m_errorString.append("Aborted");
                  break;
               }
               continue;
            }

            m_errorCode = errCode;
            m_errorString.clear().appendf("Error connecting to %s: ", netRes.lpRemoteName);

            if (showErrorMsg("Error connecting to target"))//ignore or abort?
               break;
         }
         else
            break;
      }
#else
      m_errorCode = 0;
      m_errorString.clear();
      m_warnings.clear();
#endif
      return (m_errorCode == 0);
   }
   //---------------------------------------------------------------------------
   //  disconnectTarget
   //---------------------------------------------------------------------------
   bool disconnectTarget()
   {
      m_processed = true;
      m_errorCode = 0;
      m_errorString.clear();
      m_warnings.clear();

#ifdef _WIN32
      if (m_dummy) return true;
      DWORD errCode = ::WNetCancelConnection2(getFileSpec(DT_TARGET), CONNECT_UPDATE_PROFILE, FALSE);
      if (errCode != NO_ERROR)
      {
         m_errorCode = errCode;
         m_errorString.appendf("Error disconnecting from %s: ", getFileSpec(DT_TARGET));
         formatSystemError(m_errorString, m_errorCode);
         //don't display error since deployment of one component (like esp) may involve 
         //deploying multiple others (like esp service modules) and we would get disconnection
         //failures as a result in any case.
      }
#endif      
      return (m_errorCode == 0);
   }   

   //returns true if the caller needs to ignore or abort error; false to retry
   //
   bool showErrorMsg(const char* szTitle)
   {
     if (m_machineOS != MachineOsLinux  || (m_machineOS == MachineOsLinux && m_sshUser.isEmpty() && m_sshKeyFile.isEmpty()))
     {
      if (m_errorCode == 0)
         m_errorCode = getLastError();

      if (m_errorCode != (DWORD) -1)
         formatSystemError(m_errorString, m_errorCode);
     }

      bool rc = true;//ignore
      try 
      {
            rc = m_pCallback->processException( m_processType.get(), m_compName.get(), m_instanceName.get(), NULL, 
                                                        m_errorString, szTitle, this);
      }
      catch (IException* e)
      {
         e->Release();
         m_abort = m_pCallback->getAbortStatus();
      }
      catch (...)
      {
         m_abort = m_pCallback->getAbortStatus();
      }

      return rc;
   }

   virtual void setErrorCode(DWORD code)            { m_errorCode = code;        }
   virtual void setErrorString(const char* msg)     { m_errorString = msg;       }
   virtual void setWarnings(const char* warnings)   { m_warnings = warnings;     }
   virtual void setProcessed(bool bProcessed = true){ m_processed = bProcessed;  }
   static  bool getRetryStatus()            { return s_retry;   }
   static  void setRetryStatus(bool status) { s_retry = status; }
   virtual IDeploymentCallback& getCallback() const { return *m_pCallback; }

private:
  char* getMD5Checksum(const char *filename, char* digestStr)
  {
      if (isEmptyString(filename))
          return NULL;

      if (!checkFileExists(filename))
          return NULL;

      OwnedIFile ifile = createIFile(filename);
      if (!ifile)
      return NULL;

      OwnedIFileIO ifileio = ifile->open(IFOread);
      if (!ifileio)
      return NULL;

      size32_t len = (size32_t) ifileio->size();
      if (len < 1)
      return NULL;

      char * buff = new char[1+len];
      size32_t len0 = ifileio->read(0, len, buff);
      buff[len0] = 0;

      md5_state_t md5;
      md5_byte_t digest[16];

      md5_init(&md5);
      md5_append(&md5, (const md5_byte_t *)buff, len0);
      md5_finish(&md5, digest);

      for (int i = 0; i < 16; i++)
          sprintf(&digestStr[i*2],"%02x", digest[i]);

    delete[] buff;

      return digestStr;
  }

  void getKeyPassphrase(StringBuffer& passphr)
  {
    passphr.clear().append("-pw ");
    if (m_sshKeyPassphrase.isEmpty())
      passphr.clear();
    else
    {
      StringBuffer sb;
      decrypt(sb, m_sshKeyPassphrase.str());
      passphr.append(sb.str());
    }
  }

private:
   Owned<IDeploymentCallback> m_pCallback;
   StringAttr m_caption;
   StringAttr m_compName;
   StringAttr m_instanceName;
   StringAttr m_fileSpec[2];
   StringAttr m_processName;
   StringAttr m_processType;
   StringAttr m_sshUser;
   StringAttr m_sshKeyFile;
   StringAttr m_sshKeyPassphrase;
   int m_func;
   bool m_processed;
   DWORD m_errorCode;
   StringBuffer m_errorString;
   StringBuffer m_warnings;
   bool m_targetFileWithWrongCase;
   bool m_abort;
   bool m_msgBoxOwner;//global flag set (used for multithreaded copying only)
   bool m_dummy;
   bool m_updateProgress;
   bool m_useSsh;
   EnvMachineOS m_machineOS;
   unsigned m_flags;
   static Monitor s_monitor;
   static bool s_retry;
   static bool s_msgBoxActive;
};

/*static*/ Monitor CDeployTask::s_monitor;
/*static*/ bool    CDeployTask::s_retry = false;
/*static*/ bool    CDeployTask::s_msgBoxActive = false;

//the following class implements a thread that asynchronously 
//copies a given file.  The current implementation of IDeployTask
//does not seemlessly lend itself to polymorphism since each task
//type has its own characteristic parameters and the caller must
//specifically invoke different methods like copyFile based on 
//task type.  Ideally, there needs to be only one worker method.
//
class CDeployTaskThread : public CInterface, 
                          implements IDeployTaskThread
{
public:
    IMPLEMENT_IINTERFACE;

    CDeployTaskThread()
    {
    }
    virtual ~CDeployTaskThread()
    {
    }

    virtual void init(void *startInfo) override
    {
        m_pTask.set((IDeployTask*)startInfo);
    }
    virtual void threadmain() override
    {
        m_pTask->copyFile( m_pTask->getFlags() );

        static Mutex m;
        m.lock();
        try
        {
            m_pTask->getCallback().printStatus(m_pTask);
        }
        catch(IException* e)
        {
            e->Release();
        }
        m.unlock();

        if (m_pTask && m_pTask->getAbort())
        {
            m_pTask->getCallback().printStatus(STATUS_NORMAL, NULL, NULL, NULL, "Aborting, please wait...");
            throw MakeStringException(0, "Abort");
        }
    }

    virtual bool canReuse() const override
    {
        return true;
    }
    virtual bool stop() override
    {
        return true;
    }
   
    virtual IDeployTask* getTask () const     { return m_pTask;     }
    virtual void setTask (IDeployTask* pTask) { m_pTask.set(pTask); }

    virtual bool getAbort() const      { return s_abort;   }
    virtual void setAbort(bool bAbort) { s_abort = bAbort; }
   
private:
    Owned<IDeployTask> m_pTask;
    static bool s_abort;
};

class CDeployTaskThreadFactory : public CInterface, public IThreadFactory
{
public:
    IMPLEMENT_IINTERFACE;
    IPooledThread *createNew()
    {
        return new CDeployTaskThread();
    }
};

bool CDeployTaskThread::s_abort = false;



//---------------------------------------------------------------------------
//  Factory functions
//---------------------------------------------------------------------------
IDeployTask* createDeployTask(IDeploymentCallback& callback, const char* caption, 
                              const char* processType, const char* comp, 
                              const char* instance, const char* source, 
                              const char* target, const char* sshUser, const char* sshKeyFile, 
                              const char* sshKeyPassphrase, bool useSsh, EnvMachineOS os/* = MachineOsUnknown*/, 
                              const char* processName/*=NULL*/)
{
   return new CDeployTask(callback, caption, processType, comp, instance, source, target, sshUser, sshKeyFile, 
     sshKeyPassphrase, useSsh, os, processName);
}

IThreadFactory* createDeployTaskThreadFactory()
{
   return new CDeployTaskThreadFactory();
}

void initializeMultiThreadedCopying()
{
   CDeployTask::setRetryStatus(false);
}
