This documentation provides a comprehensive guide to setting up an Ubuntu VM on Oracle VirtualBox, installing the HPCC-Platform, and preparing the environment for testing and development.

#### Setting Up an Ubuntu VM on Oracle VirtualBox

**Overview of the installation process on a Windows machine:**

1. **Download and install Oracle VirtualBox:**
    - Visit the Oracle VirtualBox [website](https://www.virtualbox.org/) and download the latest version for Windows.
    - Follow the installation instructions to install VirtualBox on your system.

2. **Download the Ubuntu 22.04 Desktop ISO file:**
    - Go to the Ubuntu [downloads page](https://ubuntu.com/download/desktop) and download the Ubuntu 22.04 LTS ISO file.

3. **Set up a new VM in VirtualBox:**
    - Open VirtualBox and click on `New` to create a new virtual machine.
    - Name the VM and select the type and version (Linux, Ubuntu 64-bit).
    - Configure system settings such as memory size and hard disk (create a virtual hard disk now).
    - Link the ISO file by going to the `Settings` of the VM, navigating to `Storage`, and attaching the ISO file to the optical drive.
    - Boot the VM and follow the installation wizard to complete the Ubuntu setup.

#### Installing HPCC-Platform on the VM

**After successfully installing the VM, proceed with installing the HPCC-Platform:**

1. **Download the HPCC-Platform package:**
   ```sh
   wget https://cdn.hpccsystems.com/releases/CE-Candidate-9.8.2/bin/platform/hpccsystems-platform-community_9.8.2-1jammy_amd64_withsymbols.deb
   ```

2. **Install the package:**
   ```sh
   sudo dpkg -i hpccsystems-platform-community_9.8.2-1jammy_amd64_withsymbols.deb
   ```

3. **Fix missing dependencies:**
   ```sh
   sudo apt-get install -f
   ```

4. **Check if the installation is successful:**
   ```sh
   sudo dpkg -l | grep 'hpccsystems-pl'
   ```
5. **Start HPCC-Platform:**
   ```sh
   sudo /etc/init.d/hpcc-init start
   ```

6. **Verify access to ECL Watch:**
    - Open a browser on your local machine and go to `http://192.168.0.221:8010/` to check if you can access ECL Watch.

#### Cloning and Checking Out the HPCC-Platform Repository on VM

1. **Clone the HPCC-Platform GitHub repository:**
   ```sh
   git clone https://github.com/hpcc-systems/HPCC-Platform.git
   ```

2. **Navigate to the repository directory:**
   ```sh
   cd HPCC-Platform
   ```

3. **Check out the specific version of HPCC-Platform:**
   ```sh
   git checkout candidate-9.8.x
   ```

#### Running Regression Test Setup

**Navigate to the testing directory and set up regression tests:**
   ```sh
   cd testing/regress
   ./ecl-test setup --preAbort '/opt/HPCCSystems/bin/smoketest-preabort.sh'
   ```

#### Running Spray Tests

**Execute the spray tests:**
   ```sh
   ./ecl-test query --preAbort /opt/HPCCSystems/bin/smoketest-preabort.sh --excludeclass python2,embedded-r,embedded-js,3rdpartyservice,mongodb *spray*
   ```

#### Generating JSON Files

**Generate JSON files for workunits, files, and DFU workunits:**
   ```sh
   curl localhost:8010/WsWorkunits/WUQuery.json | python3 -m json.tool > workunits.json
   curl localhost:8010/WsDfu/DFUQuery.json?PageSize=250 | python3 -m json.tool > files.json
   curl localhost:8010/FileSpray/GetDFUWorkunits.json | python3 -m json.tool > dfu-workunits.json
   ```

#### Transferring Files Using WinSCP

Find your json files in the VM and use the WinSCP tool to transfer files from the VM to your local machine.

#### Downloading Dependencies

1. **Download the following dependencies:**
    - [TestNG 7.7.1](https://repo1.maven.org/maven2/org/testng/testng/7.7.1/testng-7.7.1.jar)
    - [Jackson Annotations 2.17.0](https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.17.0/jackson-annotations-2.17.0.jar)
    - [Jackson Core 2.17.0](https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.17.0/jackson-core-2.17.0.jar)
    - [Jackson Databind 2.17.0](https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.17.0/jackson-databind-2.17.0.jar)
    - [JCommander 1.82](https://repo1.maven.org/maven2/com/beust/jcommander/1.82/jcommander-1.82.jar)
    - [Selenium Java 4.22.0](https://github.com/SeleniumHQ/selenium/releases/download/selenium-4.22.0/selenium-java-4.22.0.zip)
    - [SLF4J API 1.7.30](https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.30/slf4j-api-1.7.30.jar)
    - [SLF4J Simple 1.7.30](https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/1.7.30/slf4j-simple-1.7.30.jar)

#### Adding Dependencies to Your Java Code

1. Include the downloaded dependencies in your Java project.
2. Specify the path to your ChromeDriver when creating a `ChromeDriver` object in your code.

#### Writing Test Cases for ECL Watch

You are now ready to start writing your test cases for ECL Watch running at `http://192.168.0.221:8010/`.

#### Uninstall HPCC-Platform on VM

Use below commands

```sh
   cd /opt/HPCCSystems/sbin
   sudo ./complete-uninstall.sh -p
   ```
