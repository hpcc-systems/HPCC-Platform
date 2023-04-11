# Documentation files structure and usage

## Documentation build process

### Configure

1.  Find xsltproc
2.  Find fop
3.  configure custom xsl and xml files needed for document generation.

### Build

4.  Expand docbook resources

5.  

    Build docs based on calls to DOCBOOK\_TO\_PDF

    :   a.  CLEAN\_REL\_BOOK for provided xml
        b.  RUN\_XSLTPROC on xml file generated in a using defined xsl
            file to generate fo file
        c.  RUN\_FOP to generate pdf from fo file generated in b

The entire build process via a call to DOCBOOK\_TO\_PDF is run on cmake
targets that are generated with custom targets at configure time.

## Directory structure of CMake files

\- docs/ - Directory for documentation building

:   -   bin/ - Directory containing scripts used by documentation team
    -   BuildTools/ - Directory containing xsl files and xsl file
        templates used by documentors
    -   common/ - Directory containing common files used by all
        documents built
    -   resources/ - directory containing docbook resources needed to
        build documentation
