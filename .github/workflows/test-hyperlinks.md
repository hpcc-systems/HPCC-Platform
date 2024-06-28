# Test Hyperlinks
Documentation files often contains numerous hyperlinks to external resources, references, or even other internal documents. Ensuring these links are functional helps maintain the integrity of the information provided. Broken or incorrect links need to be identified before integrating. [test-hyperlinks.yml](https://github.com/hpcc-systems/HPCC-Platform/tree/master/.github/workflows/test-hyperlinks.yml) helps in finding those broken links.  
The documentation files mentioned here are:  
	1. XML (.xml)  
	2. Markdown (.md)  
	3. ReStructuredText (.rst) 

## Triggering the Workflow
The workflow can be triggered in two ways:  
1. **pull_request** : As part of CI (continuous Integration),the workflow is triggered by a Pull Request and checks only those documentation files that are newly added or modified in that PR.  
   
2. **workflow_dispatch** : It can also be triggered manually to make a complete scan of all the documentation files in the repository. Regularly triggering it manually helps in identifying those links which are broken over time in the repository. We can set the Debug-Mode to true, if we need to access all the files created during the workflow run such as `linksList.txt`, `checkedLinksCache.txt`, `xmlFilesList.txt`, `mdFilesList.txt`, `rstFilesList.txt`. These files will be helpful to check if the workflow is running as expected.

```yaml
on: 
  pull_request:
    branches:
      - "master"
      - "candidate-*"
      - "!candidate-9.4.*"
      - "!candidate-9.2.*"
      - "!candidate-9.0.*"
      - "!candidate-8.*"
      - "!candidate-7.*"
      - "!candidate-6.*"
  workflow_dispatch: 
    inputs: 
      Debug-Mode:
        type: boolean
        description:  Run in Debug mode to upload all created files
        default: false
        required: false
```
## Description about the steps:
### Checkout repository : 
Clone the HPCC-Platform repository to the working directory. Here `fetch-depth: 2` helps in getting the last 2 commits, including the merge commit made by GitHub Actions when the workflow is triggered.

```yaml
- name: Checkout repository
  uses: actions/checkout@v4
  with:
    fetch-depth: 2
```
### List Documentation files
List all the documentation files. This steps depends on how the workflow is triggered. If it is triggered through a PR, then the `else` condition is executed and only the newly updated documentation files are listed. If it is manually triggered then it lists all the documentation files. Here `HEAD` points to the latest merge commit made by GitHub Actions and `HEAD^1` points to the immediate parent commit of it.
```yaml
- name: List Documentation files
  run: |
  if [[ ${{ github.event_name }} == "workflow_dispatch" ]]; then
    find $PWD -name '*.xml' -type f > xmlFilesList.txt
    find $PWD -name '*.md' -type f > mdFilesList.txt
    find $PWD -name '*.rst' -type f > rstFilesList.txt
  else 
    git diff --name-only HEAD^1 HEAD > modifiedFilesList.txt
    cat modifiedFilesList.txt | grep -E "*.xml" > xmlFilesList.txt
    cat modifiedFilesList.txt | grep -E "*.md" > mdFilesList.txt
    cat modifiedFilesList.txt | grep -E "*.rst" > rstFilesList.txt
  fi
```
### List links from Documentation files
Here all the internal and external links are listed from the list of documentation files and stored in `linksList.txt`.  
The hyperlinks pattern used for different files:  
1. **XML**  
   All the links that are of the form ```<ulink url="link-address">link-Text</ulink>``` are listed.
2. **Markdown**  
   All the links that are of the form ```[link-text](link-address)``` and even if the link is directly mentioned without using the above syntax is also listed.
3. **reStructuredText**  
   All the links that are of the form ```.. _link-text: link-address``` and directly mentioned hyperlinks are listed too.
	
The `-H` and `-n` flags to the `grep` command outputs the file Name and line number of the link respectively.  
Note:  
1. The `<ulink` and `</ulink>` tags are also listed in XML files. Only the hyperlinks enclosed between these tags are listed, while others are ignored.
2. The \`\`\` (triple backticks represents code snippets in Markdown files ) are also listed along with the hyperlinks. So that what ever comes inside the backticks are ignored.
```yaml     
- name: List links from Documentation files
  run: |
    IFS=$'\n'
    for FILE in $( cat xmlFilesList.txt )
    do 
      #check if the file is missing
      if [[ ! -f $FILE ]]; then
        echo -e "$FILE -\u001b[31m file missing" 
        echo $FILE >> missingFiles.txt
        continue
      fi        
      grep -onHE -e "<ulink" -e 'url="http[^\"\]+' -e "</ulink>" ${FILE} | sed 's/url="//' > links.tmp
      FLAG=0
      for LINE in $( cat links.tmp )
      do 
        LINK=$( echo $LINE | cut -d ':' -f3- ) 
        if [[ ${LINK:0:6} == '<ulink' ]]; then 
          FLAG=1
          continue 
        elif [[  ${LINK:0:8} == '</ulink>' ]]; then 
          FLAG=0
          continue
        fi
        if [[ $FLAG -eq 1 ]]; then
          echo $LINE >> linksList.txt
        fi
      done  
    done
    for FILE in $( cat mdFilesList.txt )
    do
      #check if the file is missing
      if [[ ! -f $FILE ]]; then
        echo -e "$FILE -\u001b[31m file missing" 
        echo $FILE >> missingFiles.txt
        continue
      fi 
      grep -onHE -e "\]\([^\)]+" -e "\`\`\`[^\`]*" -e "http://[^\ \;\"\'\<\>\]\[\,\`\)]+" -e "https://[^\ \;\"\'\<\>\]\[\,\`\)]+" ${FILE} | sed 's/](//'  > links.tmp
      FLAG=0
      for LINE in $( cat links.tmp )
      do 
        LINK=$( echo $LINE | cut -d ':' -f3- ) 
        if [[ ${LINK:0:3} == '```' ]]; then 
          FLAG=$(( 1 - FLAG ))
          continue
        fi
        if [[ $FLAG -eq 0 ]]; then
          echo $LINE >> linksList.txt
        fi
      done
    done

    for FILE in $( cat rstFilesList.txt )
    do 
      #check if the file is missing
      if [[ ! -f $FILE ]]; then
        echo -e "$FILE -\u001b[31m file missing" 
        echo $FILE >> missingFiles.txt
        continue
      fi 
      grep -onHE -e ".. _[^\]+" -e "http://[^\ \;\"\'\<\>\,\`\)]+" -e "https://[^\ \;\"\'\<\>\,\`\)]+" ${FILE} | sed 's/.. _[^\:]*: //' >> linksList.txt 
    done
```
After listing all the hyperlinks. The list is then split into External and Internal links ignoring the localhost links.
```yaml
    if [[ -f linksList.txt ]]; then
      cat linksList.txt | grep -vE '127.0.0.1|localhost|\$|\[' | grep -E 'https://|http://' > externalLinks.txt
      cat linksList.txt | grep -vE '127.0.0.1|localhost|\$|\[' | grep -vE 'https://|http://' > internalLinks.txt
    fi
```
### Test External links
The external links are tested using `curl`  command. The `-I` tag helps in fetching only the HTTP-request header. The `-L` flag helps in redirecting to the new location specified in the Location header. After fetching the status code of each link, the link and its status code is uploaded into the `checkedLinksCache.txt`, a file that helps in caching so that redundant links are tested only once. So before checking the status code through `curl` command. We check the `checkedLinksCache.txt` file if the link exists, if so then the status code is fetched from the cache file. If the status code of the link is found to be 404, then the link is uploaded to the `error-report.log` log file.
```yaml    
- name: Test External links
  run: |
    touch checkedLinksCache.txt
    IFS=$'\n'
    if [[ -f externalLinks.txt ]]; then
      for LINE in $(cat externalLinks.txt )
      do 
        LINK=$( echo $LINE | cut -d ':' -f3- )
        LINK=${LINK%.} #remove trailing dot(.)
        LINK=${LINK% } #remove trailing space
        CHECK_CACHE=$( cat checkedLinksCache.txt | grep "$LINK~" | wc -w )
        TRY=3   #Max number of attempts to check status code of hyperlinks
        if [[ $CHECK_CACHE -eq 0  ]]; then
          while [[ $TRY -ne 0 ]]
          do
            STATUS_CODE=$(curl -LI -m 60 -s $LINK | grep "HTTP" | tail -1 | cut -d' ' -f2 ) 
            if [[ -n $STATUS_CODE ]]; then
              echo "$LINK~$STATUS_CODE" >> checkedLinksCache.txt
              break
            else  
              echo $LINE
              echo "retrying..."
              TRY=$(( TRY - 1))
            fi
          done
          else
            STATUS_CODE=$( cat checkedLinksCache.txt | grep "$LINK~" | cut -d '~' -f2 )
        fi
        if [[ $STATUS_CODE -eq 404 ]]; then 
          echo -e "${LINK} - \u001b[31m404 Error"
          echo "${LINE}" >> error-report.log
        elif [[ ! -n $STATUS_CODE ]]; then
                echo -e "${LINK} - \033[0;31mNo Response\033[0m"
                echo "${LINE}(No-Response)" >> error-report.log
        else
          echo "${LINK} - ${STATUS_CODE}" 
        fi
      done
    fi
```
### Test Internal Links
Internal links are those that point to another file in the same repository or a reference to a section in the same file. For file references, it checks whether the mentioned file exists in the specified file reference path. For internal file section references, it checks if the specified reference section is present in the documentation.
```yaml
- name: Test Internal Links
  run: |
    if [[ -f internalLinks.txt ]]; then
      for LINE in $( cat internalLinks.txt )
      do 
        REFERENCE=$( echo $LINE | cut -d ':' -f3- )
        FILE=$( echo $LINE | cut -d ':' -f1 )
        if [[ ${REFERENCE:0:1} == '#' ]]; then 
          Link_text=$( cat $FILE | grep  -oE "\[.*\]\(${REFERENCE}\)" | sed 's/\[//' | cut -d ']' -f1 )
          IS_PRESENT=$(cat $FILE | grep -oE "# ${Link_text}" | wc -w)
          if [[ $IS_PRESENT -eq 0 ]]; then 
            echo -e "${LINE} -\u001b[31m invalid reference" 
            echo "${LINE}" >> error-report.log
          fi
        else 
          if [[ ${REFERENCE:0:1} == '/' ]]; then
            BASE_DIR=$PWD
          else
            BASE_DIR=${FILE/$( basename $FILE )}
          fi
          SEARCH_FILE="$BASE_DIR/${REFERENCE}"
          SEARCH_FILE=$( realpath $SEARCH_FILE )
          if [[ ! -f $SEARCH_FILE ]]; then 
            echo -e "${LINE} -\u001b[31m invalid reference" 
            echo ${LINE/$REFERENCE/$SEARCH_FILE} >> error-report.log
          fi
        fi
      done
    fi
```
### Report Error links
After testing the status code of all the links. Stats about number of files scanned, number of unique links and the total number of references to the broken links are displayed. If at least one broken link is found, then the workflow is made to fail.
```yaml
- name: Report Error links
  run: | 
    if [[ -f error-report.log ]]; then 
      NUMBER_OF_404_LINKS=$( cat error-report.log | wc -l ) 
    fi
    echo -e "\u001b[32mNo. of files scanned : $( cat *FilesList.txt | wc -l )"
    if [[ $NUMBER_OF_404_LINKS -ne 0 ]]; then
      echo -e "\u001b[31mNo. of unique broken links : $( cat error-report.log | cut -d: -f3- | sort | uniq | wc -l )"
      echo -e "\u001b[31mTotal No. of reference to broken links : $( cat error-report.log | cut -d: -f3- | sort | wc -l )"
      exit -1  
    else 
      echo -e "\u001b[32mNo Broken-links found"
    fi
```
### Modify log file  
The log file that is generated is beautified and is made more readable. The paths are trimmed to HPCC-Platform directory. If missing files are found then they are appended to the `error-report.log` file. The files that are created during the workflow run are  uploaded as an artifact, only if it is triggered by `workflow_dispatch` with `Debug-Mode` set to true, else those files are removed and only the `error-report.log` file is uploaded.
```yaml       
- name: Modify log file
  if: ${{ failure() || cancelled() }}
  run: | 
    BASE_DIR=${PWD%$(basename $PWD)}
    BASE_DIR=$(echo $BASE_DIR | sed 's/\//\\\//g') 
    sed -i "s/${BASE_DIR}//g" error-report.log
    FILE_NAMES_LIST=$(cat error-report.log  | cut -d ':' -f1 | sort | uniq )
    FILE_COUNT=1
    for LINE in $FILE_NAMES_LIST
    do 
      LINKS_LIST=$( cat error-report.log | grep $LINE | cut -d ':' -f2- ) 
      echo "$FILE_COUNT. $LINE" >> error-reportTmp.log
      FILE_COUNT=$(( FILE_COUNT + 1))
      for LINK in $LINKS_LIST
      do 
        echo -e "\t Line $LINK" | sed 's/:/ : /' >> error-reportTmp.log
      done
    done
    if [[ $(cat missingFiles.txt | wc -w ) -eq 0 ]]; then 
      echo -e "Broken links: \n" > error-report.log
      cat error-reportTmp.log >> error-report.log
    else 
      echo -e "Missing Files: \n" > error-report.log
      cat missingFiles.txt >> error-report.log
      echo -e "Broken links: \n" >> error-report.log
      cat error-reportTmp.log >> error-report.log
    fi 
    if [[ ${{ github.event_name }} == "pull_request" || ${{ inputs.Debug-Mode }} == false ]]; then 
      rm -rf *FilesList.txt \
             checkedLinksCache.txt \
             *Links.txt \
             linksList.txt \
    fi
```
### Upload logs
The generated `error-report.log` file is uploaded as an artifact. If it is triggered by workflow_dispatch with `Debug-Mode:` set to true, then the remaining files, that are created during workflow run are not removed in the previous step and hence those files are also uploaded as an artifact.
```yaml
- name: Upload logs
  uses: actions/upload-artifact@v4
  if: ${{ failure() || cancelled() }}
  with:
    name: Hyperlinks-testing-log
    path: |
      /home/runner/work/HPCC-Platform/HPCC-Platform/error-report.log
      /home/runner/work/HPCC-Platform/HPCC-Platform/*FilesList.txt
      /home/runner/work/HPCC-Platform/HPCC-Platform/checkedLinksCache.txt
      /home/runner/work/HPCC-Platform/HPCC-Platform/*Links.txt
      /home/runner/work/HPCC-Platform/HPCC-Platform/linksList.txt
  if-no-files-found: ignore
``` 