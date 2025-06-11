# Purpose
Purpose of this testing workflow is to validate the changes of the Helm Charts used in generating the Kubernetes resources needed by the HPCC Platform
  - Ensures Helm templates generate Kubernetes yaml files correctly
  - catches any unexpected changes in the generated kubernetes files
  - Analyisis of kube-score for any poor practices or miscongurations of the kubernetes YAML files. Essentialy checks for best practice of these files

# Prerequisites
Before this test can run, the following are needed:
- Helm CLI Tool(command-interfacee for working with helm, essentially different tools when working with helm charts)
- run.sh(a script that valides Helm harts through a series of checks)
- kube-score(analysis tool which reviews kubernetes YAMl files such as deployments, services, configmaps,etc)

# Steps
1. pre_job
   - Checks whether Helm-related files were changed in the pull requests
   - Outputs a flag to decide whether build should run(helm: true/false
2. build job(if helm changes are detected)
   - install Helm
   - install kube-score
   - run.sh script
5. Generate the Helm templates from current pull request branch
6. Generate now the base version of the helm template from the base branch for comparison
7. Finally check the differences of the pull request helm template vs the base helm template
   - If they are the same templates, compare the test YAMl variations one by one
   - If any differences are found conversely then fail the workflow
# Tools
 - Helm: helm charts used to generate the Kubernetes YAML file
 - kube-score: again for analysis of the kubernetes YAML files
 - run.sh: for validation of the Helm charts
 - git: generates and compares the pull request helm templates from the pull request vs main branch

   

