/*##############################################################################
    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#include "jexcept.hpp"
#include "jfile.hpp"
#include "jmisc.hpp"
#include "jcontainerized.hpp"


namespace k8s {

static StringBuffer myPodName;

const char *queryMyPodName()
{
    return myPodName;
}

KeepJobs translateKeepJobs(const char *keepJob)
{
    if (!isEmptyString(keepJob)) // common case
    {
        if (streq("podfailures", keepJob))
            return KeepJobs::podfailures;
        else if (streq("all", keepJob))
            return KeepJobs::all;
    }
    return KeepJobs::none;
}

bool isActiveService(const char *serviceName)
{
    VStringBuffer getEndpoints("kubectl get endpoints %s \"--output=jsonpath={range .subsets[*].addresses[*]}{.ip}{'\\n'}{end}\"", serviceName);
    StringBuffer output;
    runKubectlCommand("checkEndpoints", getEndpoints.str(), nullptr, &output);
    // Output should be zero or more lines each with an IP
    return (output.length() && output.charAt(0) != '\n');
}

void deleteResource(const char *componentName, const char *resourceType, const char *job)
{
    VStringBuffer resourceName("%s-%s-%s", componentName, resourceType, job);
    resourceName.toLowerCase();
    VStringBuffer deleteResource("kubectl delete %s/%s", resourceType, resourceName.str());
    runKubectlCommand(componentName, deleteResource, nullptr, nullptr);

    // have to assume command succeeded (if didn't throw exception)
    // NB: file will only exist if autoCleanup used (it's okay not to exist)
    StringBuffer jobName(job);
    jobName.toLowerCase();
    VStringBuffer k8sResourcesFilename("%s,%s,%s.k8s", componentName, resourceType, jobName.str());
    remove(k8sResourcesFilename);
}

bool checkExitCodes(StringBuffer &output, const char *podStatuses)
{
    const char *startOfPodStatus = podStatuses;
    while (*startOfPodStatus)
    {
        const char *endOfPodStatus = strchr(startOfPodStatus, '|');
        StringBuffer podStatus;
        if (endOfPodStatus)
            podStatus.append((size_t)(endOfPodStatus-startOfPodStatus), startOfPodStatus);
        else
            podStatus.append(startOfPodStatus);
        StringArray fields;
        fields.appendList(podStatus, ",");
        if (3 == fields.length()) // should be 3 fields {<exitCode>,<"initContainer"|"container">,<name>}
        {
            const char *exitCodeStr = fields.item(0);
            if (strlen(exitCodeStr))
            {
                unsigned exitCode = atoi(exitCodeStr);
                if (exitCode) // non-zero = failure
                {
                    output.appendf(" %s '%s' failed with exitCode = %u", fields.item(1), fields.item(2), exitCode);
                    return true;
                }
            }
        }
        if (!endOfPodStatus)
            break;
        startOfPodStatus = endOfPodStatus+1;
    }
    return false;
}

void waitJob(const char *componentName, const char *resourceType, const char *job, unsigned pendingTimeoutSecs, KeepJobs keepJob)
{
    VStringBuffer jobName("%s-%s-%s", componentName, resourceType, job);
    jobName.toLowerCase();
    VStringBuffer waitJob("kubectl get jobs %s -o jsonpath={.status.active}", jobName.str());
    VStringBuffer getScheduleStatus("kubectl get pods --selector=job-name=%s --output=jsonpath={.items[*].status.conditions[?(@.type=='PodScheduled')].status}", jobName.str());

    unsigned delay = 100;
    unsigned start = msTick();

    bool schedulingTimeout = false;
    Owned<IException> exception;
    try
    {
        for (;;)
        {
            StringBuffer output;
            runKubectlCommand(componentName, waitJob, nullptr, &output);
            if ((0 == output.length()) || streq(output, "0"))  // status.active value
            {
                // Job is no longer active - we can terminate
                DBGLOG("kubectl jobs output: %s", output.str());
                VStringBuffer checkJobExitStatus("kubectl get jobs %s '-o=jsonpath={range .status.conditions[*]}{.type}: {.status} - {.message}|{end}'", jobName.str());
                runKubectlCommand(componentName, checkJobExitStatus, nullptr, &output.clear());
                if (strstr(output.str(), "Failed: "))
                {
                    VStringBuffer errMsg("Job %s failed [%s].", jobName.str(), output.str());
                    VStringBuffer checkInitContainerExitCodes("kubectl get pods --selector=job-name=%s '-o=jsonpath={range .items[*].status.initContainerStatuses[*]}{.state.terminated.exitCode},{\"initContainer\"},{.name}{\"|\"}{end}'", jobName.str());
                    runKubectlCommand(componentName, checkInitContainerExitCodes, nullptr, &output.clear());
                    DBGLOG("checkInitContainerExitCodes - output = %s", output.str());
                    if (!checkExitCodes(errMsg, output))
                    {
                        // no init container failures, check regular containers
                        VStringBuffer checkContainerExitCodes("kubectl get pods --selector=job-name=%s '-o=jsonpath={range .items[*].status.containerStatuses[*]}{.state.terminated.exitCode},{\"container\"},{.name}{\"|\"}{end}'", jobName.str());
                        runKubectlCommand(componentName, checkContainerExitCodes, nullptr, &output.clear());
                        DBGLOG("checkContainerExitCodes - output = %s", output.str());
                        checkExitCodes(errMsg, output);
                    }
                    throw makeStringException(0, errMsg);
                }
                else // assume success, either .status.conditions type of "Complete" or "Succeeded"
                    break;
            }
            runKubectlCommand(nullptr, getScheduleStatus, nullptr, &output.clear());

            // Check whether pod has been scheduled yet - if resources are not available pods may block indefinitely waiting to be scheduled, and
            // we would prefer them to fail instead.
            bool pending = streq(output, "False");
            if (pendingTimeoutSecs && pending && msTick()-start > pendingTimeoutSecs*1000)
            {
                schedulingTimeout = true;
                VStringBuffer getReason("kubectl get pods --selector=job-name=%s \"--output=jsonpath={range .items[*].status.conditions[?(@.type=='PodScheduled')]}{.reason}{': '}{.message}{end}\"", jobName.str());
                runKubectlCommand(componentName, getReason, nullptr, &output.clear());
                throw makeStringExceptionV(0, "Failed to run %s - pod not scheduled after %u seconds: %s ", jobName.str(), pendingTimeoutSecs, output.str());
            }
            MilliSleep(delay);
            if (delay < 10000)
                delay = delay * 2;
        }
    }
    catch (IException *e)
    {
        EXCLOG(e, nullptr);
        exception.setown(e);
    }
    if (keepJob != KeepJobs::all)
    {
        // Delete jobs unless the pod failed and keepJob==podfailures
        if ((nullptr == exception) || (KeepJobs::podfailures != keepJob) || schedulingTimeout)
            deleteResource(componentName, "job", job);
    }
    if (exception)
        throw exception.getClear();
}

bool applyYaml(const char *componentName, const char *wuid, const char *job, const char *resourceType, const std::list<std::pair<std::string, std::string>> &extraParams, bool optional, bool autoCleanup)
{
    StringBuffer jobName(job);
    jobName.toLowerCase();
    VStringBuffer jobSpecFilename("/etc/config/%s-%s.yaml", componentName, resourceType);
    StringBuffer jobYaml;
    try
    {
        jobYaml.loadFile(jobSpecFilename, false);
    }
    catch (IException *E)
    {
        if (!optional)
            throw;
        E->Release();
        return false;
    }
    jobYaml.replaceString("_HPCC_JOBNAME_", jobName.str());

    VStringBuffer args("\"--workunit=%s\"", wuid);
    args.append(" \"--k8sJob=true\"");
    for (const auto &p: extraParams)
    {
        if (hasPrefix(p.first.c_str(), "_HPCC_", false)) // job yaml substitution
            jobYaml.replaceString(p.first.c_str(), p.second.c_str());
        else
            args.append(" \"--").append(p.first.c_str()).append('=').append(p.second.c_str()).append("\"");
    }
    jobYaml.replaceString("_HPCC_ARGS_", args.str());

    runKubectlCommand(componentName, "kubectl replace --force -f -", jobYaml, nullptr);

    if (autoCleanup)
    {
        // touch a file, with naming convention { componentName },{ resourceType },{ jobName }.k8s
        // it will be used if the job fails ungracefully, to tidy up leaked resources
        // normally (during graceful cleanup) these resources and files will be deleted by deleteResource
        VStringBuffer k8sResourcesFilename("%s,%s,%s.k8s", componentName, resourceType, jobName.str());
        touchFile(k8sResourcesFilename);
    }

    return true;
}

static constexpr unsigned defaultPendingTimeSecs = 600;
void runJob(const char *componentName, const char *wuid, const char *jobName, const std::list<std::pair<std::string, std::string>> &extraParams)
{
    Owned<IPropertyTree> compConfig = getComponentConfig();
    KeepJobs keepJob = translateKeepJobs(compConfig->queryProp("@keepJobs"));
    unsigned pendingTimeoutSecs = compConfig->getPropInt("@pendingTimeoutSecs", defaultPendingTimeSecs);

    bool removeNetwork = applyYaml(componentName, wuid, jobName, "networkpolicy", extraParams, true, true);
    applyYaml(componentName, wuid, jobName, "job", extraParams, false, KeepJobs::none == keepJob);
    Owned<IException> exception;
    try
    {
        waitJob(componentName, "job", jobName, pendingTimeoutSecs, keepJob);
    }
    catch (IException *e)
    {
        EXCLOG(e, nullptr);
        exception.setown(e);
    }
    if (removeNetwork)
        deleteResource(componentName, "networkpolicy", jobName);
    if (exception)
        throw exception.getClear();
}

// returns a vector of {pod-name, node-name} vectors,
// represented as a nested vector for extensibility, e.g. to add other meta fields
std::vector<std::vector<std::string>> getPodNodes(const char *selector)
{
    VStringBuffer getWorkerNodes("kubectl get pods --selector=job-name=%s \"--output=jsonpath={range .items[*]}{.metadata.name},{.spec.nodeName}{'\\n'}{end}\"", selector);
    StringBuffer result;
    runKubectlCommand("get-worker-nodes", getWorkerNodes, nullptr, &result);

    if (result.isEmpty())
        throw makeStringExceptionV(-1, "No worker nodes found for selector '%s'", selector);

    const char *start = result.str();
    const char *finger = start;
    std::string fieldName;
    std::vector<std::vector<std::string>> results;
    std::vector<std::string> current;
    while (true)
    {
        switch (*finger)
        {
            case ',':
            {
                if (start == finger)
                    throw makeStringException(-1, "getPodNodes: Missing node name(s) in output");
                fieldName.assign(start, finger-start);
                current.emplace_back(std::move(fieldName));
                finger++;
                start = finger;
                break;
            }
            case '\n':
            case '\0':
            {
                if (start == finger)
                    throw makeStringException(-1, "getPodNodes: Missing pod name(s) in output");
                fieldName.assign(start, finger-start);
                current.emplace_back(std::move(fieldName));
                results.emplace_back(std::move(current));
                if ('\0' == *finger)
                    return results;
                finger++;
                start = finger;
                break;
            }
            default:
            {
                ++finger;
                break;
            }
        }
    }
}

static unsigned podInfoInitCBId = 0;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    auto updateFunc = [&](const IPropertyTree *oldComponentConfiguration, const IPropertyTree *oldGlobalConfiguration)
    {
        if (myPodName.length()) // called at config load time, and never needs to be refreshed
            return;
        // process pod information from environment
        getEnvVar("MY_POD_NAME", myPodName.clear());
    };
    if (isContainerized())
        podInfoInitCBId = installConfigUpdateHook(updateFunc, true);
    return true;
}
MODULE_EXIT()
{
    if (isContainerized())
        removeConfigUpdateHook(podInfoInitCBId);
}

} // end of k8s namespace
