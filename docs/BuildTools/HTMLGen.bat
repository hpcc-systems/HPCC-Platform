rem this batch file generates HTML pages for the three BIG Books of the Portal

xsltproc -xinclude ../BuildTools/PortalGen.xsl ../EN_US/ECLLanguageReference/ECLR-Includer.xml

xsltproc -xinclude ../BuildTools/PortalGen.xsl ../EN_US/ECLStandardLibraryReference/SLR-Includer.xml

xsltproc -xinclude ../BuildTools/PortalGen.xsl ../EN_US/ECLProgrammersGuide/PrGd-Includer.xml


rem  for compiling CHM
rem 

xsltproc -xinclude -nonet --stringparam html.stylesheet ${css_file_name} --stringparam generate.toc "book toc"  --param use.id.as.filename 1 --param chapter.autolabel 0 ../../docbook-xsl/htmlhelp/htmlhelp.xsl  ./ECLReference.xml 