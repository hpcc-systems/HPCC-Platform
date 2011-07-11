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
#include "deploy.hpp"
#include "jptree.hpp"  
#include "jstring.hpp"  
#include <time.h>
#include "jfile.hpp"


class CDeployLog : public CInterface, implements IDeployLog
{
public:
   IMPLEMENT_IINTERFACE;
   //---------------------------------------------------------------------------
   //  CDeployLog
   //---------------------------------------------------------------------------
   CDeployLog( IDeploymentCallback& callback, const char* filename, const char* envFilename )
    : m_pCallback(&callback),
      m_filename(filename)
   {
      assertex(filename);
      m_tree.setown(createPTree("DeployLog", false));
      m_tree->addProp("@environmentArchive", envFilename);
      m_tree->addProp("@startTime", getTimestamp());
   }
   //---------------------------------------------------------------------------
   //  ~CDeployLog
   //---------------------------------------------------------------------------
   virtual ~CDeployLog()
   {
      m_tree->addProp("@stopTime", getTimestamp());
      writeLog();
   }
   //---------------------------------------------------------------------------
   //  addTask
   //---------------------------------------------------------------------------
   IPropertyTree* addTask(IDeployTask& task)
   {
      // Get or add Component node
      IPropertyTree* compNode = addComponent(task.getCompName());
      assertex(compNode);

      // Get Tasks node
      IPropertyTree* tasksNode = compNode->queryPropTree("Tasks");
      assertex(tasksNode);

      // Add new task
      IPropertyTree* node = createPTree("Task", false);
      node->addProp("@action", task.getCaption());
      node->addProp("@source", task.getFileSpec(DT_SOURCE));
      node->addProp("@target", task.getFileSpec(DT_TARGET));

      CDateTime modifiedTime;
      Owned<IFile> pTargetFile = createIFile(task.getFileSpec(DT_TARGET));
      
      if (pTargetFile->getTime(NULL, &modifiedTime, NULL))
      {
         StringBuffer timestamp;
         modifiedTime.getString(timestamp);
         offset_t filesize = pTargetFile->size();

         node->addProp("@date", timestamp.str());
         node->addPropInt64("@size", filesize);
      }

      if (task.getErrorCode())
      {
         node->addProp("@error", task.getErrorString());
         compNode->setProp("@error", "true");
      }
      return tasksNode->addPropTree("Task", node);
   }
   //---------------------------------------------------------------------------
   //  addDirList
   //---------------------------------------------------------------------------
   IPropertyTree* addDirList(const char* comp, const char* path)
   {
      // Get or add Component node
      IPropertyTree* compNode = addComponent(comp);
      assertex(compNode);

      // Add new Directory node
      assertex(path);
      char ppath[_MAX_PATH];
      strcpy(ppath, path);
      removeTrailingPathSepChar(ppath);
      IPropertyTree* node = createPTree("Directory", false);
      node->addProp("@name", ppath);
      getDirList(ppath, node);
      return compNode->addPropTree("Directory", node);
   }

private:
   //---------------------------------------------------------------------------
   //  addComponent
   //---------------------------------------------------------------------------
   IPropertyTree* addComponent(const char* comp)
   {
      assertex(comp);

      // Add component node if necessary
      Owned<IPropertyTreeIterator> iter = m_tree->getElements("./Component");
      for (iter->first(); iter->isValid(); iter->next())
      {
         if (strcmp(comp, iter->query().queryProp("@name"))==0)
            return &iter->query();
      }

      // If node not found, create it and add Tasks subnode
      IPropertyTree* compNode = createPTree("Component", false);
      compNode->addProp("@name", comp);
      compNode->addPropTree("Tasks", createPTree("Tasks", false));
      return m_tree->addPropTree("Component", compNode);
   }
   //---------------------------------------------------------------------------
   //  getDirList
   //---------------------------------------------------------------------------
   void getDirList(const char* path, IPropertyTree* parentNode)
   {
      Owned<IDirectoryIterator> pDirIter = createDirectoryIterator(path, "*");
      ForEach(*pDirIter)
      {
         IFile& iFile = pDirIter->query();
         const char* dirEntryName = iFile.queryFilename();

         // Process directories, but not the "." and ".." directories
         if (iFile.isDirectory() && *dirEntryName != '.')
         {  
            // Create prop tree, add to parent, and recurse
            StringBuffer newPath(path);
            newPath.append('\\').append(dirEntryName);

            IPropertyTree* node = createPTree("Directory", false);
            node->addProp("@name", newPath.str());

            getDirList(newPath.str(), parentNode->addPropTree("Directory", node));
         }
         else
         {
            // Create prop tree and add to parent
            CDateTime modifiedTime;
            iFile.getTime(NULL, &modifiedTime, NULL);

            StringBuffer timestamp;
            modifiedTime.getString(timestamp);

            offset_t filesize = iFile.size();

            IPropertyTree* node = createPTree("File", false);
            node->addProp("@name", dirEntryName);
            node->addProp("@date", timestamp.str());
            node->addPropInt64("@size", filesize);
            parentNode->addPropTree("File", node);
         }
      }
   }
   //---------------------------------------------------------------------------
   //  getTimestamp
   //---------------------------------------------------------------------------
   const char* getTimestamp()
   {
      time_t t = time(NULL);
      struct tm* now = localtime(&t);
      strftime(m_timestamp, sizeof(m_timestamp), "%Y-%m-%d %H:%M:%S", now);
      return m_timestamp;
   }
   //---------------------------------------------------------------------------
   //  writeLog
   //---------------------------------------------------------------------------
   void writeLog()
   {
      StringBuffer xml;
      toXML(m_tree, xml);

      Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Create File", NULL, NULL, NULL, NULL, 
        m_filename, "", "", "", false);
      task->createFile(xml.str());

      Owned<IFile> pFile = createIFile(m_filename);
      pFile->setReadOnly(true);
   }

private:
   Owned<IDeploymentCallback> m_pCallback;
   Owned<IPropertyTree> m_tree;
   StringAttr m_filename;
   char m_timestamp[40];
};

//---------------------------------------------------------------------------
//  Factory functions
//---------------------------------------------------------------------------
IDeployLog* createDeployLog(IDeploymentCallback& callback, const char* filename, const char* envFilename)
{
   return new CDeployLog(callback, filename, envFilename);
}
