package com.hpccsystems.jdbcdriver;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

public class HPCCLogicalFiles
{
	private Properties files;
	private List<String> superfiles;

	public HPCCLogicalFiles()
	{
		files = new Properties();
		superfiles = new ArrayList<String>();
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
		return (DFUFile)files.get(filename);
	}

	public Enumeration<Object> getFiles()
	{
		return files.elements();
	}

	public void updateSuperFiles()
	{
		int superfilescount = superfiles.size();
		int superfilesupdated = 0;

		for(int i = 0; i < superfilescount; i++)
		{
			DFUFile superfile = (DFUFile) files.get(superfiles.get(i));
			if (!superfile.hasFileRecDef())
			{
				if(superfile.containsSubfiles())
				{
					List<String> subfiles = superfile.getSubfiles();
					for (int y = 0; y < subfiles.size(); y++)
					{
						DFUFile subfile =((DFUFile) files.get(subfiles.get(y)));
						if (subfile.hasFileRecDef())
						{
							superfile.setFileRecDef(subfile.getFileRecDef("recdef"));

							if(superfile.hasFileRecDef())
							{
								System.out.println(superfile.getFullyQualifiedName() + " set to use record definition from subfile " + subfile.getFullyQualifiedName());
								superfilesupdated++;
								break;
							}
						}
					}
				}
			}
		}
		System.out.println("Update superfiles' record definition ( " + superfilesupdated + " out of " + superfilescount + " )");
	}
}
