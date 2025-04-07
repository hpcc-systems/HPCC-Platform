# Bundle Testing on thor 
## Why do we need Bundle testing?
Bundle testing on Thor involves regression testing of machine learning bundles. This process is crucial for assessing the performance of Thor. It's a highly sensitive test, it identifies even minor changes or issues within Thor, ensuring that any potential problems are caught early.

## How and when is it triggered?
The [bundleTest-thor.yml](https://github.com/hpcc-systems/HPCC-Platform/blob/master/.github/workflows/bundleTest-thor.yml) is triggered by a workflow call. This call occurs within the [build-vcpkg.yml](https://github.com/hpcc-systems/HPCC-Platform/blob/master/.github/workflows/build-vcpkg.yml).

```Yaml
on:
  workflow_call:
```
The [build-vcpkg.yml](https://github.com/hpcc-systems/HPCC-Platform/blob/master/.github/workflows/build-vcpkg.yml) makes a workflow call to [bundleTest-thor.yml](https://github.com/hpcc-systems/HPCC-Platform/blob/master/.github/workflows/bundleTest-thor.yml) through `uses: ./.github/workflows/bundleTest-thor.yml` when a pull request is made.


```yaml
test-bundles-on-thor-ubuntu-22_04:
if: ${{ contains('pull_request,push', github.event_name) }}
needs: build-docker-ubuntu-22_04
uses: ./.github/workflows/bundleTest-thor.yml
with:
    os: ubuntu-22.04
    asset-name: 'docker-ubuntu-22_04'
    generate-zap: ""
secrets: inherit
```
The HPCC platform needs to be built on the latest commit and the artifact for installation should be available before making a workflow call to the [bundleTest-thor.yml](https://github.com/hpcc-systems/HPCC-Platform/blob/master/.github/workflows/bundleTest-thor.yml). To achieve this we add `needs: build-docker-ubuntu-22_04` to the workflow call step.

## Passing inputs to the workflow

We can pass inputs to the [bundleTest-thor.yml](https://github.com/hpcc-systems/HPCC-Platform/blob/master/.github/workflows/bundleTest-thor.yml)(called workflow) from the [build-vcpkg.yml](https://github.com/hpcc-systems/HPCC-Platform/blob/master/.github/workflows/build-vcpkg.yml)(caller workflow) using `inputs:`
```yaml
on:
  workflow_call:
    inputs:
      os:
        type: string
        description: 'Operating System'
        required: false
        default: 'ubuntu-22.04'
      asset-name:
        type: string
        description: 'Asset Name'
        required: false
        default: 'build-docker-package'
      dependencies:
        type: string
        description: 'Dependencies'
        required: false
        default: 'bison flex build-essential binutils-dev curl lsb-release libcppunit-dev python3-dev default-jdk r-base-dev r-cran-rcpp r-cran-rinside r-cran-inline libtool autotools-dev automake git cmake xmlstarlet'
      get-stat: 
        type: boolean
        description: 'Run Query stat'
        required: false
        default: false
      generate-zap:
        type: string
        description: 'Generate ZAP files'
        required: false
        default: ''
      test-core-file-generation: 
        type: boolean
        description: 'Test core file generation'
        required: false
        default: false
```
- **os** : Specifies the desired operating system of the runner machine using `runs-on: ${{ inputs.os }}`, with the default being Ubuntu-22.04.
- **asset-name** : It specifies the name of the built artifact of the HPCC Platform. 
- **dependencies** : Lists the required dependencies to install and start the platform.
- **get-stat** : This is boolean input that decides whether the QueryStat2.py step should be executed or not. The default value is false.
- **generate-zap** : Indicates if ZAP reports are needed for specific test cases. The default is an empty string.You can either pass the name of the test file with .ecl extension or * at the end. The later is used to refer the workunit name. Eg. "KMeansValidateOBT-240531-133100" can be refered as "KMeansValidateOBT*" 
Example:   
  Caller Workflow: [build-vcpkg.yml](https://github.com/hpcc-systems/HPCC-Platform/blob/master/.github/workflows/build-vcpkg.yml)
    ```yaml
    test-bundles-on-thor-ubuntu-22_04:
        if: ${{ contains('pull_request,push', github.event_name) }}
        needs: build-docker-ubuntu-22_04
        uses: ./.github/workflows/bundleTest-thor.yml
        with:
          os: ubuntu-22.04
          asset-name: 'docker-ubuntu-22_04'
          generate-zap: "'KMeansValidateOBT.ecl SVTest*'"
        secrets: inherit
    ```
  **NOTE:** The double quotes are required if * notation is used to specify the name of the test. It makes sure that string function properly during the workflow run.
- **test-core-file-generation** :A boolean input that determines whether the core file generation test step should be executed. It helps us to know whether the core handler is working fine as expected when a core file is generated during the tests. 

## Environmental Variables: 
```yaml
env:
  ML_SUPPRESS_WARNING_FILES: "RegressionTestModified.ecl ClassificationTestModified.ecl"
  ML_EXCLUDE_FILES: "--ef ClassicTestModified.ecl,SVCTest.ecl,ClassificationTestModified.ecl"
  BUNDLES_TO_TEST: "ML_Core PBblas GLM GNN DBSCAN LearningTrees TextVectors KMeans SupportVectorMachines LinearRegression LogisticRegression"       
  UPLOAD_ARTIFACT: false 
  LIST_BUNDLES_DYNAMICALLY: true
  SET_FAILURE_STATUS: false
```
- `ML_SUPPRESS_WARNING_FILES:`Specifies the files that require a warning suppression parameter injection into the ECL code before they are executed.
- `ML_EXCLUDE_FILES:` The files specified here are excluded during the run.
- `BUNDLES_TO_TEST:` Lists the bundles to test.
- `UPLOAD_ARTIFACT:` Determines whether the logs Artifact should be uploaded or not.
- `LIST_BUNDLES_DYNAMICALLY:` This enables us to turn on or off the dynamic listing of ECL Bundles feature. If in future, the README.rst file in `hpcc-systems/ecl-bundles` repository undergoes a complete modification, we could turn off dynamic listing of ECL Bundles feature, till we align with the new modifications.
- `SET_FAILURE_STATUS:` It will be updated to true, if any bundle installation fails, if any bundles' test case fails or if core files are generated. This helps in deciding the status of the workflow run. If it is updated to true then the workflow will be marked as failed.
## Steps Involved:
The steps in the workflow run on the specified operating system, with Ubuntu-22.04 as the default.
- **Download Package**  
This step enables us to download the ready-to-install HPCC Platform's artifact built on the latest commit.
  ```yaml
      - name: Download Package
        uses: actions/download-artifact@v4
        with:
          name: ${{ inputs.asset-name }}
          path: ${{ inputs.asset-name }}
  ```
- **Install Dependencies**  
Install the dependencies that are necessary for the platform to install and start sucessfully.
  ```yaml
  - name: Install Dependencies
    shell: "bash"
    run: |
        sudo apt-get update
        sudo apt-get install -y \
          git \
          wget \
          net-tools \
          tzdata \
          unzip \
          xvfb \
          libxi6 \
          default-jdk \
          gdb \
          ${{ inputs.dependencies }}
  ```
- **Install Package**  
Install the HPCC Platform from the downloaded artifact, set permissions and configure the Thor engine to use 2 slaves.
  ```yaml
  - name: Install Package
    shell: "bash"
    run: |
        sudo apt-get install -y -f ./${{ inputs.asset-name }}/*.deb
        sudo chown -R $USER:$USER /opt/HPCCSystems
        sudo chown -R $USER:$USER /var/*/HPCCSystems
        sudo xmlstarlet ed -L -u 'Environment/Software/ThorCluster/@slavesPerNode' -v 2 -u 'Environment/Software/ThorCluster/@channelsPerSlave' -v 1 /etc/HPCCSystems/environment.xml
  ```
- **Checkout ecl-bundles repository**  
To update the `BUNDLES_TO_TEST:` list dynamically. We scrap the latest list directly from the `hpcc-systems/ecl-bundles` repository's README.rst file. This step clones `hpcc-systems/ecl-bundles` repository to the working directory.
  ```yaml
  - name: Checkout ecl-bundles repository
    if: ${{ env.LIST_BUNDLES_DYNAMICALLY == 'true' }}
    uses: actions/checkout@v4
    with:
      repository: hpcc-systems/ecl-bundles
  ```
- **Scrap Bundles List**  
From the [README.rst](https://github.com/hpcc-systems/ecl-bundles/blob/master/README.rst) file in `hpcc-systems/ecl-bundles` repository. We update the `BUNDLES_TO_TEST:` list dynamically. In the [README.rst](https://github.com/hpcc-systems/ecl-bundles/blob/master/README.rst) file, the ecl-bundles data is present in table format. And in reStructuredText(.rst) files, tables are represented by `'|'` pipe character. So to extract the tables data, we `grep` all the lines that start with a `'|'`(pipe character). Along with tables data we also extract `===========`(a continuos string of `=` represents headings in reStructredText(.rst) files.) this helps in differentiating different sections/tables in the file. The extracted data is stored in `TABLES_DATA`. In the [README.rst](https://github.com/hpcc-systems/ecl-bundles/blob/master/README.rst) file the ecl-bundles data is present in the second section of the file, so we set `HEADER_NUM_OF_ECL_BUNDLES=2`. Using this information, we extract the ecl-bundles' names and the repository address from `TABLES_DATA`.
  ```yaml
  - name: Scrap Bundles List
    if: ${{ env.LIST_BUNDLES_DYNAMICALLY == 'true' }}
    shell: "bash"
    run: | 
      if [[ -f README.rst ]] 
      then 
          # append all table information into TABLES_DATA from README.rst 
          # tables are created using '|' (pipe character) in reStructuredText(.rst) files. So we are extracting all the lines that start with '|'(pipe character).
          # along with tables, we also extract '============='. This is used to represent headings in .rst files. This helps us to differentiate between different sections/tables of the README file.
          TABLES_DATA=$( cat README.rst | grep -oE -e "\|[^\|]*\|"  -e "==.*==" | sed 's/|//g' )
          IFS=$'\n'
          HEADER_NUM_OF_ECL_BUNDLES=2
          HEADER_COUNT=0
          for LINE in $TABLES_DATA
          do
            LINE=${LINE# }   #removing space from beginning of the line
            LINE=${LINE%% [^A-Za-z0-9]*} #remove trailing spaces.
            if [[ ${LINE:0:1} == "=" ]]; then 
              HEADER_COUNT=$(( HEADER_COUNT + 1 ))
              continue
            fi
            if [[ $HEADER_COUNT -eq $HEADER_NUM_OF_ECL_BUNDLES ]]; then 
              if [[ ${LINE:0:4} == "http" ]]; then
                echo -e "Bundle Repo : ${LINE}\n" 
                BUNDLE=$( basename $LINE )
                BUNDLES_TO_TEST+=" ${BUNDLE/.git}" 
              else
                echo "Bundle Name : ${LINE}" 
              fi
            elif [[ $HEADER_COUNT -eq $(( HEADER_NUM_OF_ECL_BUNDLES + 1 )) ]]; then 
              break
            fi
          done
          BUNDLES_TO_TEST=$(echo $BUNDLES_TO_TEST | sed 's/ /\n/g' | sort -bf -u )
          #                ( print bundles list   | replace all spaces with new-line | sort the list and select unique items )
          BUNDLES_TO_TEST=${BUNDLES_TO_TEST//$'\n'/ } # replace all newline characters with spaces
          echo "BUNDLES TO TEST : $BUNDLES_TO_TEST"
          echo "BUNDLES_TO_TEST=$BUNDLES_TO_TEST" >> $GITHUB_ENV
      else 
          echo "README.rst file not found! in HPCC-Systems/ecl-bundles repository" 
      fi
  ```
- **Install ML Dependencies**  
Install the necessary Machine learning library dependecies.
  ```yaml
  - name: Install ML Dependencies
    shell: "bash"
    run: |  
        sudo apt install libsvm-dev libsvm-tools 
        sudo pip install tensorflow numpy keras   
  ```
- **Start HPCC-Platform**  
Setup the core generation. `ulimit -c 100` sets the maximum size of core files that can be generated by a process when it crashes. `echo 'core_%e.%p' | sudo tee /proc/sys/kernel/core_pattern` sets the pattern for the filenames of core dumps generated by the system. Where `%e` and `%p` is replaced by the filename and the process ID (PID) of the crashing process. Set the LANG and update the system locale settings. And start the HPCC Platform.
  ```yaml
  - name: Start HPCC-Platform
    shell: "bash"
    run: |
        ulimit -c 100
        echo 'core_%e.%p' | sudo tee /proc/sys/kernel/core_pattern
        export LANG="en_US.UTF-8"
        sudo update-locale
        sudo /etc/init.d/hpcc-init start
  ```
- **Core generation test**  
This step is used to check whether the core handler is working fine as expected when a core file is generated during the tests. Here we force generate a core dump file by running an ECL script (crash.ecl) on hthor that sends a signal to terminate the process. This is an optional step. This step runs only when it is specified in the inputs via workflow call.
  ```yaml
  - name: Core generation test
    if: ${{ inputs.test-core-file-generation }}
    shell: "bash"
    run: |
        echo > crash.ecl << EOF
        boolean seg() := beginc++ #option action
        #include <csignal>
        #body
        raise(SIGABRT);
        return false;
        endc++;
        output(seg()); 
        EOF
        
        ecl run -t hthor crash.ecl
    continue-on-error: true
  ```
- **Get test from GitHub**  
Install the ECL bundles specified in ` BUNDLES_TO_TEST` from GitHub.

  ```yaml
   - name: Get test from Github
     shell: "bash"
     run: |
        IFS=' ' read -a BUNDLES_TO_TEST <<< $BUNDLES_TO_TEST
        BUNDLES_COUNT=${#BUNDLES_TO_TEST[@]}
        for ((i=0; i<$BUNDLES_COUNT; i++))
        do
            BUNDLE_NAME=${BUNDLES_TO_TEST[i]}
            BUNDLE_REPO="https://github.com/hpcc-systems/${BUNDLES_TO_TEST[i]}.git"
            INSTALL_CMD="ecl bundle install --update --force ${BUNDLE_REPO}"
            echo -e "\nBundle Name : ${BUNDLE_NAME}"
            echo "Bundle Repo : ${BUNDLE_REPO}"
            tryCountMax=5
            tryCount=$tryCountMax
            tryDelay=1m

            while true
            do
                # Initialize a flag to indicate if bundle install failed, by default set it to false.
                bundleInstallFailed=false
                # Attempt to execute the INSTALL_CMD command and capture its output.
                cRes=$( ${INSTALL_CMD} 2>&1 ) ||  bundleInstallFailed=true
                # If the command fails (i.e., INSTALL_CMD returns a non-zero exit status), the bundleInstallFailed flag is set to true.
                # This flag indicates that there was an error during the bundle installation process.
                if [[ $bundleInstallFailed == true ]]
                then
                    tryCount=$(( $tryCount-1 ))

                    if [[ $tryCount -ne 0 ]]
                    then
                        sleep ${tryDelay}
                        continue
                    else
                        mkdir -p /home/runner/HPCCSystems-regression/log/ 
                        touch Failed_bundle_install.summary
                        echo "Install $BUNDLE_NAME bundle was failed after ${tryCountMax} attempts. Result is: ${cRes}" | tee /home/runner/HPCCSystems-regression/log/Failed_bundle_install.summary
                        echo "UPLOAD_ARTIFACT=true" >> $GITHUB_ENV
                        echo "SET_FAILURE_STATUS=true" >> $GITHUB_ENV
                        break;
                    fi
                else
                    echo "Install $BUNDLE_NAME bundle was success." 
                    BUNDLE_VERSION=$( echo "${cRes}" | egrep "^$BUNDLE_NAME" | tail -1 | awk '{ print $2 }' )
                    echo "Version: $BUNDLE_VERSION" 
                    break
                  fi
            done
        done
  ```
- **Run Tests**  
The bundle testing process is initiated and the logs are processed through the `ProcessLog()` function to identify any failures. The `ProcessLog` takes the bundle name and the target cluster as the input. It checks the log file and greps the number of test cases that failed. If it is non-zero then `Failed_test.summary` is updated and `uploadArtifact` is set to true. More information about regression testing can be found [here](https://github.com/hpcc-systems/HPCC-Platform/blob/master/testing/regress/README.rst).
  ```yaml
  - name: Run Tests
    id: run
    shell: "bash"
    working-directory: /home/runner/.HPCCSystems/bundles/_versions/
    run: |
        ProcessLog()
        { 
            BUNDLE=$1
            TARGET=$2
            logfilename=$( ls -clr /home/runner/HPCCSystems-regression/log/thor.*.log | head -1 | awk '{ print $9 }' )
            failed=$(cat ${logfilename} | sed -n "s/^[[:space:]]*Failure:[[:space:]]*\([0-9]*\)[[:space:]]*$/\1/p")                           
        
            if [[ "$failed" -ne 0 ]]
            then 
                echo "Bundle : ${BUNDLE}" >> /home/runner/HPCCSystems-regression/log/Failed_test.summary
                cat ${logfilename} >> /home/runner/HPCCSystems-regression/log/Failed_test.summary
                echo "UPLOAD_ARTIFACT=true" >> $GITHUB_ENV
                echo "SET_FAILURE_STATUS=true" >> $GITHUB_ENV
            fi
            # Rename result log file to name of the bundle
            logname=$(basename $logfilename)
            bundlelogfilename=${logname//$TARGET/$BUNDLE}
            printf "%s, %s\n" "$logname" "$bundlelogfilename"
            mv -v $logfilename /home/runner/HPCCSystems-regression/log/ml-$bundlelogfilename
        }
        IFS=' ' read -a BUNDLES_TO_TEST <<< $BUNDLES_TO_TEST
        while read bundle
        do
            bundleRunPath=${bundle%/ecl}                         # remove '/ecl' from the end of the $bundle
            bundlePath=${bundleRunPath%/OBTTests};       # remove '/OBTTests' from the end of the $bundleRunPath if exists
            bundleName=${bundlePath%/test}                    # remove '/test' from the end of the $bundlePath if exists
            bundleName=$(basename $bundleName )         # remove path from $bundleName
            
            if [[ "$bundle" =~ "LearningTrees" ]]
            then
                # add a warning supression parameter in the file
                for file in $ML_SUPPRESS_WARNING_FILES
                do
                    if [[ $( egrep -c '#ONWARNING\(30004' $bundle/$file ) -eq 0 ]]
                    then
                        pushd $bundle 
                        cp -fv $file $file-back
                        # Insert a comment and the "#ONWARNING" after the Copyright header
                        sed -i '/## \*\//a \\n// Patched by the bundleTest on '"$( date '+%Y.%m.%d %H:%M:%S')"' \n#ONWARNING(30004, ignore); // Do not report execute time skew warning' $file
                        popd
                    fi
                done
            fi
            if [[ ! "${BUNDLES_TO_TEST[*]}" =~ "$bundleName"  ]]
            then
                continue
            fi
            pushd $bundleRunPath
            /opt/HPCCSystems/testing/regress/ecl-test run -t thor --config /opt/HPCCSystems/testing/regress/ecl-test.json --timeout 3600 -fthorConnectTimeout=3600 --pq 1 $ML_EXCLUDE_FILES
            retCode=$( echo $? )
            if [ ${retCode} -eq 0 ] 
            then
                ProcessLog "$bundleName" "thor"
            fi
            popd
        done< <(find . -iname 'ecl' -type d | sort )
    continue-on-error: true
  ```
- **Generate ZAP files**  
ZAP report files are generated for specified files if mentioned in the input. If none are mentioned, this step is skipped.

  ```yaml
  - name: Generate ZAP files
    if: ${{ ! inputs.generate-zap == '' }} 
    run: |  
        IFS=' ' read -a ML_GENERATE_ZAP_FOR <<< "${{ inputs.generate-zap }}"
        if [  ${#ML_GENERATE_ZAP_FOR[@]} -ne 0 ]
        then
            for test in ${ML_GENERATE_ZAP_FOR[*]}
            do 
                test=${test/.ecl/*}
                wuid=$(ecl getwuid -n $test --limit 1)
                if [[ -n $wuid ]]
                then
                    ecl zapgen $wuid  --path /home/runner/HPCCSystems-regression/zap --inc-thor-slave-logs
                    echo "testName : ${test}  wuid : ${wuid}" >> zap.summary
                    cp zap.summary /home/runner/HPCCSystems-regression/zap 
                    echo "UPLOAD_ARTIFACT=true" >> $GITHUB_ENV
                fi
            done 
        fi
  ```
- **Check for Core files**  
If core files are generated, create a stack trace using the gdb command. The generated .trace files are stored in the logs path.
  ```yaml
  - name: Check for Core files
    run: |
        CORE_FILES=( $(sudo find /var/lib/HPCCSystems/ -iname 'core*' -mtime -1 -type f -exec printf "%s\n" '{}' \; ) )
        echo "NUM_OF_ML_CORES=${#CORE_FILES[@]}" >> $GITHUB_ENV
        if [ ${#CORE_FILES[@]} -ne 0 ]
        then      
            for  core in ${CORE_FILES[@]}
            do
                base=$( dirname $core )
                lastSubdir=${base##*/}
                comp=${lastSubdir##my}
                sudo gdb --batch --quiet -ex "set interactive-mode off" -ex "echo \n Backtrace for all threads\n==========================" -ex "thread apply all bt" -ex "echo \n Registers:\n==========================\n" -ex "info reg" -ex "echo \n Disas:\n==========================\n" -ex "disas" -ex "quit" "/opt/HPCCSystems/bin/${comp}" $core | sudo tee "$core.trace" 2>&1
                cp "$core.trace" /home/runner/HPCCSystems-regression/log/ 
            done
            echo "UPLOAD_ARTIFACT=true" >> $GITHUB_ENV
            echo "SET_FAILURE_STATUS=true" >> $GITHUB_ENV

        fi
  ```
- **Get test stat**  
This step generates test statistics, allowing comparison and analysis of test performance on the specified cluster.
  ```yaml
    - name: Get test stat
      if: ${{ inputs.get-stat }}
      run: |
          ./QueryStat2.py -p /home/runner/HPCCSystems-regression/log/  -d '' -a --timestamp --compileTimeDetails 1 --graphTimings --allGraphItems --addHeader
          NUM_OF_STAT_FILES=$( find /home/runner/HPCCSystems-regression/log/ -type f -iname "*.csv" -o -iname "*.cfg" | wc -l )
          if [[ $NUM_OF_STAT_FILES -ne 0 ]]
          then 
              echo "UPLOAD_ARTIFACT=true" >> $GITHUB_ENV
          fi
  ```
- **Check failure status**  
This step runs only when `SET_FAILURE_STATUS` is set to true. The workflow is made to fail in the following cases:  
  1. Bundle installation fails
  2. Test cases fail during bundle test run
  3. Core files are generated during the run
  ```yaml
    - name: Check failure status
    if: ${{ env.SET_FAILURE_STATUS == 'true' }}
    run: |
      BUNDLE_INSTALL_SUMMARY_FILE="/home/runner/HPCCSystems-regression/log/Failed_bundle_install.summary"
      FAILED_TEST_SUMMARY_FILE=/home/runner/HPCCSystems-regression/log/Failed_test.summary
      NUM_OF_ML_CORES=${{ env.NUM_OF_ML_CORES }}
      if [[ -f $BUNDLE_INSTALL_SUMMARY_FILE ]]
      then 
          echo "Error! in Installing ECL Bundles"
          echo "Check the bundle installation summary for details: $BUNDLE_INSTALL_SUMMARY_FILE"
          cat $BUNDLE_INSTALL_SUMMARY_FILE
      fi
      if [[ -f $FAILED_TEST_SUMMARY_FILE ]]
      then 
          echo "Error: Some ECL Bundle Tests Failed"
          echo "Check the test summary for failed test cases: $FAILED_TEST_SUMMARY_FILE"
          cat $FAILED_TEST_SUMMARY_FILE
      fi
      if [[ ${#NUM_OF_ML_CORES[@]} -ne 0 ]]
      then 
          echo "Error: Core files are generated"
          echo "Check the uploaded artifact for trace files"
      fi
      exit -1
  ```

- **ml-thor-test-logs-artifact**  
If any logs, ZAP reports, or .trace files are generated, they are uploaded as artifacts for further analysis.
  ```yaml
    - name: ml-thor-test-logs-artifact
      if: ${{ failure() || cancelled() || env.UPLOAD_ARTIFACT == 'true' }}
      uses: actions/upload-artifact@v4
      with:
        name: ${{ inputs.asset-name }}-bundle-test-logs
        path: |
          /home/runner/HPCCSystems-regression/log/*
          /home/runner/HPCCSystems-regression/zap/*
        if-no-files-found: ignore
  ```

