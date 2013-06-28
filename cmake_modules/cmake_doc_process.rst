Doc build process:

Configure:
1. Find xsltproc
2. Find fop
3. configure custom xsl and xml files needed for document generation.

Build:
4. Expand docbook resources
5. build docs based on calls to DOCBOOK_TO_PDF
    a. CLEAN_REL_BOOK for provided xml
    b. RUN_XSLTPROC on xml file generated in a using defined xsl file to generate fo file
    c. RUN_FOP to generate pdf from fo file generated in b

The entire build process via a call to DOCBOOK_TO_PDF is run on cmake targets that are generated with custom targets at configure time.