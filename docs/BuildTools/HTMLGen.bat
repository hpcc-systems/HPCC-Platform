rem this file can be executed as is - as long as everything it needs is relative
rem best practice is to use the %HPCC-Platform%/docs/build directory as delivered from git
rem 
rem this batch file {section} generates HTML pages for the three BIG Books of the Portal aka the ECLReference set

xsltproc -xinclude ../BuildTools/PortalGen.xsl ../EN_US/ECLLanguageReference/ECLR-Includer.xml

xsltproc -xinclude ../BuildTools/PortalGen.xsl ../EN_US/ECLStandardLibraryReference/SLR-Includer.xml

xsltproc -xinclude ../BuildTools/PortalGen.xsl ../EN_US/ECLProgrammersGuide/PrGd-Includer.xml

rem this next code is used
rem  for compiling CHM
rem 

xsltproc -xinclude -nonet --stringparam html.stylesheet ${css_file_name} --stringparam generate.toc "book toc"  --param use.id.as.filename 1 --param chapter.autolabel 0 ../../docbook-xsl/htmlhelp/htmlhelp.xsl  ./ECLReference.xml 


rem
rem  Following code samples are here aid in doc unit testing: 
rem    To compile Formating Object (FO) files from docbook XML:
rem
rem     xsltproc --nonet --xinclude --output fo_book.fo <path_to_the_xsl_stylesheet>/fo.xsl sourcexml_book_name.xml 
rem 
rem
rem    to create PDF from (resulting) fo_book.fo 
rem > fop fo_book.fo book.pdf
rem 
rem    Can also use FOP to create PDF from docbook XML (not recommended):  
rem > fop -xml <input_file>.xml -xsl <stylesheet=fo.xsl> -pdf <out_file>.pdf          
rem
rem   NOTE: PLEASE REVIEW And understand the relative directory structure dependencies in order for these tools to work properly 
rem   
rem
