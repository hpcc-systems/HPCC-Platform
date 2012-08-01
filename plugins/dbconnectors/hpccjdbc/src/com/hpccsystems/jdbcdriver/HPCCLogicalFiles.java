package com.hpccsystems.jdbcdriver;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

public class HPCCLogicalFiles
{
    private Properties   files;
    private List<String> superfiles;
    private long         reportedFileCount;

    public HPCCLogicalFiles()
    {
        files = new Properties();
        superfiles = new ArrayList<String>();

        reportedFileCount = 0;
    }

    public void putFile(String fullyQualifiedName, DFUFile file)
    {
        files.put(fullyQualifiedName, file);
        if (file.isSuperFile())
            superfiles.add(fullyQualifiedName);
    }

    public void putFile(DFUFile file)
    {
        files.put(file.getFullyQualifiedName(), file);
        if (file.isSuperFile())
            superfiles.add(file.getFullyQualifiedName());
    }

    public boolean containsFileName(String filename)
    {
        return files.containsKey(filename);
    }

    public DFUFile getFile(String filename)
    {
        return (DFUFile) files.get(filename);
    }

    public Enumeration<Object> getFiles()
    {
        return files.elements();
    }

    private String getSubfileRecDef(DFUFile superfile)
    {
        String eclrecdef = "";

        List<String> subfiles = superfile.getSubfiles();
        for (int y = 0; y < subfiles.size(); y++)
        {
            DFUFile subfile = ((DFUFile) files.get(subfiles.get(y)));
            if (subfile.hasFileRecDef())
            {
                eclrecdef = subfile.getFileRecDef("recdef");
                System.out.println("\tUsing record definition from: " + subfile.getFullyQualifiedName());
                break;
            }
            else if (subfile.isSuperFile())
            {
                eclrecdef = getSubfileRecDef(subfile);
                if (!eclrecdef.equals(""))
                    break;
            }
        }

        return eclrecdef;
    }

    public void updateSuperFile(String superfilename)
    {
        DFUFile superfile = (DFUFile) files.get(superfilename);
        if (!superfile.hasFileRecDef())
        {
            if (superfile.containsSubfiles())
            {
                System.out.println("Processing superfile: " + superfile.getFullyQualifiedName());
                superfile.setFileRecDef(getSubfileRecDef(superfile));
                files.put(superfile.getFullyQualifiedName(), superfile);
            }
        }
    }

    public void updateSuperFiles()
    {
        int superfilescount = superfiles.size();
        int superfilesupdated = 0;

        for (int i = 0; i < superfilescount; i++)
        {
            DFUFile superfile = (DFUFile) files.get(superfiles.get(i));
            if (!superfile.hasFileRecDef())
            {
                if (superfile.containsSubfiles())
                {
                    System.out.println("Processing superfile: " + superfile.getFullyQualifiedName());
                    superfile.setFileRecDef(getSubfileRecDef(superfile));
                    if (superfile.hasFileRecDef())
                    {
                        files.put(superfile.getFullyQualifiedName(), superfile);
                        superfilesupdated++;
                    }
                }
            }
        }
        System.out.println("Update superfiles' record definition ( " + superfilesupdated + " out of " + superfilescount
                + " )");
    }

    public long getReportedFileCount()
    {
        return reportedFileCount;
    }

    public void setReportedFileCount(long reportedFileCount)
    {
        this.reportedFileCount = reportedFileCount;
    }

    public void setReportedFileCount(String reportedFileCountStr)
    {
        this.reportedFileCount = HPCCJDBCUtils.stringToLong(reportedFileCountStr, 0);
    }

    public int getCachedFileCount()
    {
        return files.size();
    }

    public int getCachedSuperFileCount()
    {
        return superfiles.size();
    }
}
