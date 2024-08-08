# Test Hyperlinks

## Table of Contents

- [Test Hyperlinks](#test-hyperlinks)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Triggering the Workflow](#triggering-the-workflow)
  - [Description about the steps:](#description-about-the-steps)
    - [Checkout repository](#checkout-repository)
    - [List Documentation files](#list-documentation-files)
    - [List links from Documentation files](#list-links-from-documentation-files)
    - [Test External links](#test-external-links)
    - [Test Internal Links](#test-internal-links)
    - [Report Error links](#report-error-links)
    - [Modify log file](#modify-log-file)
    - [Upload logs](#upload-logs)
  - [Possible reasons for failure in hyperlinks testing action](#possible-reasons-for-failure-in-hyperlinks-testing-action)
  - [Steps to integrate hyperlinks testing workflow in other workflows](#steps-to-integrate-hyperlinks-testing-workflow-in-other-workflows)

## Introduction
Documentation files often contain numerous hyperlinks to external resources, references, or even other internal documents. Ensuring these links are functional helps maintain the integrity of the information provided. Broken or incorrect links need to be identified before integrating. [test-hyperlinks.yml](https://github.com/hpcc-systems/HPCC-Platform/tree/master/.github/workflows/test-hyperlinks.yml) helps in finding those broken links.  
The documentation files mentioned here are:  
	1. XML (.xml)  
	2. Markdown (.md)  
	3. ReStructuredText (.rst) 

## Triggering the Workflow
The workflow can be triggered in three ways:  
1. **pull_request** : As part of CI (continuous Integration),the workflow is triggered by a Pull Request and checks only those documentation files that are newly added or modified in that PR.  
   
2. **workflow_call** : This workflow can be called by other workflows. It accepts the following inputs:
  - `event-type` (string, optional) : Specifies the type of event triggering the workflow. Defaults to "workflow_call".  
    Note: The default input `workflow_call` should not be changed, it is only added to differentiate between workflow_call and other event triggers.
  - `file-path` (string, optional) : Path(s) to the directory or file to be processed. Multiple paths can be separated by commas (e.g., docs/EN_US,docs/PT_BR). By default it only checks [docs](/docs) directory.
  - `file-type` (string, optional) : Specifies the types of files to be scanned. Multiple file types can be separated by commas (e.g., xml,md).By default it only checks xml.
  - `debug-mode` (boolean, optional) : If true, runs the workflow in debug mode and uploads all created files.

3. **workflow_dispatch** : This workflow can be manually triggered from the GitHub Actions tab in the repository. It accepts the following inputs:

  - `file-path` (string, optional) : Path(s) to the directory or file to be processed. Multiple paths can be separated by commas (e.g., docs/EN_US,devdoc/). By default it scans the complete repository.
  - `file-type` (string, optional) : Specifies the types of files to be scanned. Multiple file types can be separated by commas (e.g., xml,md). By deafult it checks all the file types i.e. xml,md and rst
  - `debug-mode` (boolean, optional) : If true, runs the workflow in debug mode and uploads all created files.

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

  workflow_call:
    inputs: 
      event-type:
        type: string
        default: "workflow_call"
        required: false
      file-path:
        type: string
        description: Specify the path for the directory or file. To specify multiple directories or files, separate them by Commas(,). Eg. docs/EN_US,docs/PT_BR
        default: "docs/"
        required: false
      file-type:
        type: string
        description: Specify the files which need to be scanned (md/xml/rst). To specify multiple file types separate them by Commas(,). Eg. xml,md
        default: 'xml'
        required: false  
      debug-mode:
        type: boolean
        description:  Run in Debug mode to upload all created files
        default: false
        required: false

  workflow_dispatch: 
    inputs: 
      file-path:
        type: string
        description: Specify the path for the directory or file. To specify multiple directories or files, separate them by Commas(,). Eg. docs/EN_US,devdoc/
        default: "/"
        required: false
      file-type:
        type: string
        description: Specify the files which need to be scanned (md/xml/rst). To specify multiple file types separate them by Commas(,). Eg. xml,md
        default: 'xml,md,rst'
        required: false  
      debug-mode:
        type: boolean
        description:  Run in Debug mode to upload all created files
        default: false
        required: false 
```

## Description about the steps:
### Checkout repository
Clone the HPCC-Platform repository to the working directory. Here `fetch-depth: 2` helps in getting the last 2 commits, including the merge commit made by GitHub Actions when the workflow is triggered.

```yaml
- name: Checkout repository
  uses: actions/checkout@v4
  with:
    repository: hpcc-systems/HPCC-Platform
    fetch-depth: 2
```

### List Documentation files
This steps lists the documentation files. The listing of documentation files is dependent on the event that triggered this workflow. If it is triggered by `workflow_call` or `workflow_trigger` then it lists the documentation files based on the input parameters. Or if it is triggered by a `pull_request` then it lists only the files that are added or modified in PR that triggered this workflow.  
In case of workflow_call, the event type of called workflow shares the same event as the caller workflow. For example, imagine an xyz workflow was triggered by a pull_request and in that xyz workflow there is a call to this hyperlinks testing workflow, in such case the hyperlinks testing workflow's runner environment's ${{ github.event_name }} reflects as pull_request and not workflow_call. And since we have to differentiate between different events that trigger this workflow. We have added an extra input to the workflow_call event, this is only needed to know if the event was a workflow_call. So if ${{ inputs.event_type }} is not null, it indicates that the workflow was triggered by a workflow_call.

```yaml
- name: List Documentation files
  run: |
    # Determine the event type that triggered this workflow
    # When a workflow is triggered by `workflow_call`, it doesn't explicitly provide
    # the event type of the call. Instead, it shares the event context of the calling workflow.
    # To identify if the workflow was triggered by `workflow_call`, we use an input parameter
    # called `event-type`. If this input is provided, it helps us identify that the workflow was 
    # triggered by `workflow_call`. If the input is not present, we use the github.event_name to determine the event.
    if [ -n "${{ inputs.event-type }}" ]; then
      EVENT_TYPE="${{ inputs.event-type }}"
    else
      EVENT_TYPE="${{ github.event_name }}"
    fi
    touch xmlFilesList.txt mdFilesList.txt rstFilesList.txt
    if [[ "${EVENT_TYPE}" == "workflow_dispatch" || "${EVENT_TYPE}" == "workflow_call"  ]]; then
      IFS=',' read -a DIR_LIST <<< "${{ inputs.file-path }}"
      IFS=',' read -a FILE_TYPE_LIST <<< "${{ inputs.file-type }}"
      for DIR in ${DIR_LIST[@]}
      do
        DIR=${PWD}/${DIR}             #gets the complete path
        DIR=$( realpath ${DIR} )      #gets the actual path ex: HPCC-Platform//docs --> HPCC-Platform/docs
        if [[ -f ${DIR} ]]; then        #if the specified path points to a file append it to respective list
          FILE_TYPE=${DIR##*.}          #extract the file extension
          echo ${DIR} | tee -a ${FILE_TYPE}FilesList.txt
          continue
        fi
        for FILE_TYPE in ${FILE_TYPE_LIST[@]}
        do
          FILE_TYPE=${FILE_TYPE#.}        #remove leading dot(.) if present
          FILE_TYPE=${FILE_TYPE,,}        #convert the FILE_TYPE to lowercase
          find ${DIR} -name "*.${FILE_TYPE}" -type f | tee -a ${FILE_TYPE}FilesList.txt
          # remove if any duplicate files are present
          sort -u ${FILE_TYPE}FilesList.txt -o ${FILE_TYPE}FilesList.txt
        done
      done
    elif [[ "${EVENT_TYPE}" == "pull_request" ]]; then
      git diff --name-only HEAD^1 HEAD > updatedFiles.txt
      cat updatedFiles.txt | grep -E "*.xml" | tee xmlFilesList.txt
      cat updatedFiles.txt | grep -E "*.md"  | tee mdFilesList.txt
      cat updatedFiles.txt | grep -E "*.rst" | tee rstFilesList.txt
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
2. Triple backticks ("\`\`\`") in Markdown files are used to represent code snippets. These backticks are also extracted along with hyperlinks, allowing us to distinguish between code snippets and documentation. Code snippets are ignored in processing because their syntax should not interfere with Markdown syntax. This is crucial since the logic for testing hyperlinks depends on correctly interpreting the Markdown file’s syntax.
3. Since the hyperlinks testing logic ignores the content enclosed inside tipple backticks. If there are any dummy links that resemble an actual link and are used only for the purpose of giving an example, then it is recommended to enclose them in triple backticks. For example: ```https://This/is/not/a/valid/link.com```. The dummy link used here is just for explanation and doesn't point to any external site, so, it is enclosed by triple backticks and thus is ignored during the hyperlinks testing process. Note: To see the backticks around the dummy link, view this file raw format.
4. Using triple backticks for purposes other than representing code snippets or dummy links is not recommended in Markdown files. If using triple backticks is unavoidable, ensure they are either in a pair ( i.e. there is both opening and closing triple backticks ) or escaped with a backslash ("\\") if using a single set of triple backticks to prevent misinterpretation by the hyperlinks testing logic.

```yaml     
- name: List links from Documentation files
  run: |
    IFS=$'\n'
    touch missingFiles.txt
    for FILE in $( cat xmlFilesList.txt )
    do 
      #check if the file is missing
      if [[ ! -f $FILE ]]; then
        echo -e "$FILE -\e[31m file missing\e[0m" 
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
        echo -e "$FILE -\e[31m file missing\e[0m" 
        echo $FILE >> missingFiles.txt
        continue
      fi  
      grep -onHE -e "\]\([^\)]+" -e "\`\`\`" -e "http://[^\ \;\"\'\<\>\,\`\)]+" -e "https://[^\ \;\"\'\<\>\,\`\)]+" ${FILE} | sed 's/](//'  > links.tmp
      FLAG=0
      for LINE in $( cat links.tmp )
      do 
        LINK=$( echo $LINE | cut -d ':' -f3- ) 
        if [[ ${LINK:0:3} == "\`\`\`" ]]; then 
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
        echo -e "$FILE -\e[31m file missing\e[0m" 
        echo $FILE >> missingFiles.txt
        continue
      fi  
      grep -onHE -e ".. _[^\]+" -e "http://[^\ \;\"\'\<\>\,\`\)]+" -e "https://[^\ \;\"\'\<\>\,\`\)]+" ${FILE} | sed 's/.. _[^\:]*: //' >> linksList.txt 
    done
```
After listing all the hyperlinks. The list is then split into External and Internal links ignoring the localhost links.
```yaml
  if [[ -f linksList.txt ]]; then
    echo "External links: "
    cat linksList.txt | grep -vE '127.0.0.1|localhost|\$|\[' | grep -E 'https://|http://' | tee externalLinks.txt
    echo -e "\nInternal links: "
    cat linksList.txt | grep -vE '127.0.0.1|localhost|\$|\[' | grep -vE 'https://|http://' | tee internalLinks.txt
  fi
```

### Test External links
The external links are tested using `curl`  command. The html body is ignored and dumped into /dev/null. The `-L` flag helps in redirecting to the new location specified in the Location header. After fetching the status code of each link, the link and its status code is uploaded into the `checkedLinksCache.txt`, a file that helps in caching so that redundant links are tested only once. So before checking the status code through `curl` command. We check the `checkedLinksCache.txt` file if the link exists, if so then the status code is fetched from the cache file. If the status code of the link is found to be 404, then the link is uploaded to the `error-report.log` log file.

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
        TRY=3   #Max attempts to check status code of hyperlinks
        if [[ $CHECK_CACHE -eq 0  ]]; then
          while [[ $TRY -ne 0 ]]
          do
            HTTP_RESPONSE_CODE=$( curl -o /dev/null -m 60 -sL -w "%{response_code}" $LINK ) || true
            if [[ $HTTP_RESPONSE_CODE -ne 0 ]]; then
              echo "$LINK~$HTTP_RESPONSE_CODE" >> checkedLinksCache.txt
              break
            else  
              echo $LINE
              echo "retrying..."
              TRY=$(( TRY - 1))
            fi
          done
        else
            HTTP_RESPONSE_CODE=$( cat checkedLinksCache.txt | grep "$LINK~" | cut -d '~' -f2 )
        fi
        if [[ $HTTP_RESPONSE_CODE -eq 404 ]]; then 
          echo -e "${LINK} - \e[31m404 Error\e[0m"
          echo "${LINE}" >> error-report.log
        elif [[ $HTTP_RESPONSE_CODE -eq 0 ]]; then
          HTTP_ERROR_MESSAGE=$( curl -o /dev/null -m 60 -sSL $LINK 2>&1) || true
          echo -e "${LINK} - \e[31m${HTTP_ERROR_MESSAGE}\e[0m"
          HTTP_ERROR_MESSAGE=$( echo $HTTP_ERROR_MESSAGE | sed 's/ /-/g' )
          echo "${LINE}(${HTTP_ERROR_MESSAGE})" >> error-report.log
        else
          echo "${LINK} - ${HTTP_RESPONSE_CODE}" 
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
          else
            echo -e "${LINE} -\e[32m valid reference\e[0m"
          fi
        else 
          if [[ ${REFERENCE:0:1} == '/' ]]; then
            BASE_DIR=$PWD
          else
            BASE_DIR=${FILE/$( basename $FILE )}
          fi
          SEARCH_PATH="$BASE_DIR/${REFERENCE}"
          SEARCH_PATH=$( realpath $SEARCH_PATH )
          # if it is neither a valid file nor valid a directory, then it is an invalid reference
          if [[ ! -f $SEARCH_PATH && ! -d $SEARCH_PATH ]]; then 
            echo -e "${LINE} -\u001b[31m invalid reference" 
            echo ${LINE/$REFERENCE/$SEARCH_PATH} >> error-report.log
          else
            echo -e "${LINE} -\e[32m valid reference\e[0m"
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
    echo -e "\e[32mNo. of files scanned : $( cat *FilesList.txt | wc -l )\e[0m"
    if [[ $NUMBER_OF_404_LINKS -ne 0 ]]; then
      echo -e "\e[31mNo. of unique broken links : $( cat error-report.log | cut -d: -f3- | sort | uniq | wc -l )\e[0m"
      echo -e "\e[31mTotal No. of reference to broken links : $( cat error-report.log | cut -d: -f3- | sort | wc -l )\e[0m"
      echo  "Checkout the log artifact in the summary page for more details about the broken links."
      echo  "Note: If any of the reported broken links are just example links or placeholders and are not valid links, please enclose them in triple backticks to ignore them."
      echo  "For example: \`\`\`https://This/is/not/a/valid/link.com\`\`\`" 
      exit -1  
    else
      echo -e "\e[32mNo Broken-links found\e[0m"
    fi
```

### Modify log file  
The log file that is generated is beautified and is made more readable. The paths are trimmed to HPCC-Platform directory. If missing files are found then they are appended to the `error-report.log` file. The files that are created during the workflow run are  uploaded as an artifact, if it is triggered by `workflow_dispatch` or `workflow_call`  with `debug-mode` set to true, else those files are removed and only the `error-report.log` file is uploaded.
```yaml       
- name: Modify log file
  if: ${{ failure() || cancelled() }}
  run: | 
    BASE_DIR=${PWD}
    BASE_DIR=$(echo $BASE_DIR | sed 's/\//\\\//g') 
    sed -i "s/${BASE_DIR}/HPCC-Platform/g" error-report.log
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
      echo -e "Missing Files:" > error-report.log
      FILE_COUNT=1
      for FILE in $( cat missingFiles.txt )
      do  
          echo -e "${FILE_COUNT}. ${FILE}" >> error-report.log
          FILE_COUNT=$(( FILE_COUNT + 1 ))
      done
      echo -e "\nBroken links: \n" >> error-report.log
      cat error-reportTmp.log >> error-report.log
    fi 
    if [ -z ${{ inputs.debug-mode }} ]; then
      DEBUG_MODE=false
    else
      DEBUG_MODE=${{ inputs.debug-mode }}
    fi
    if [[ ${{ github.event_name }} == "pull_request" || $DEBUG_MODE == false ]]; then 
      rm -rf *FilesList.txt \
          checkedLinksCache.txt \
          *Links.txt \
          linksList.txt 
    fi
```

### Upload logs
The generated `error-report.log` file is uploaded as an artifact. If it is triggered by workflow_dispatch or workflow_call and has `debug-mode:` set to true, then the remaining files, that are created during workflow run are not removed in the previous step and hence those files are also uploaded as an artifact. These files help in debugging the run, it would be helpful in knowing whether all the steps are functioning as expected because in debug-mode we upload all the files that were created during the run.
```yaml
- name: Upload logs
  uses: actions/upload-artifact@v4
  if: ${{ failure() || cancelled() || inputs.debug-mode == 'true'}}
  with:
    name: Hyperlinks-testing-log
    path: |
      ./error-report.log
      ./*FilesList.txt
      ./checkedLinksCache.txt
      ./*Links.txt
      ./linksList.txt
    if-no-files-found: ignore
``` 

## Possible reasons for failure in hyperlinks testing action

The below are the possible reasons why the hyperlinks testing workflow may fail,
1. Detects broken links or invalid file paths.  
   Soultion: Fix the broken links or file paths reported in the error-report.log, the log file can be found in the workflow summary as a GitHub Artifact.
2. Dummy links detected.  
   Soultion: Enclose the link in triple backticks. For example: ```https://This/is/not/a/valid/link.com```. The dummy link used here is just for explanation and doesn't point to any external site, so, it is enclosed by triple backticks and thus is ignored during the hyperlinks testing process. Note: To see the backticks around the dummy link, view this file raw format.
3. Links Returning a Non-Zero Exit Code  
   Solution: These links typically indicate issues such as connection timeouts or unsafe links. To resolve the problem, correct or update the problematic links.

## Steps to integrate hyperlinks testing workflow in other workflows

It is possible to make a `workflow_call` to the hyperlinks testing workflow from other Action workflows. Add the following code snippet to the desired workflow under the `jobs:` sections and alter the inputs as required.

```yaml
  test-hyperlinks:
    uses: ./.github/workflows/test-hyperlinks.yml
    with:
      file-path: "/docs"
      file-type: "xml"
      debug-mode: true
```

