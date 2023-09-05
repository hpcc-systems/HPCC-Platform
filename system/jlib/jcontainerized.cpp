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

#ifdef _CONTAINERIZED
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

void waitJob(const char *componentName, const char *resourceType, const char *job, unsigned pendingTimeoutSecs, KeepJobs keepJob)
{
    VStringBuffer jobName("%s-%s-%s", componentName, resourceType, job);
    jobName.toLowerCase();
    VStringBuffer waitJob("kubectl get jobs %s -o jsonpath={.status.active}", jobName.str());
    VStringBuffer getScheduleStatus("kubectl get pods --selector=job-name=%s --output=jsonpath={.items[*].status.conditions[?(@.type=='PodScheduled')].status}", jobName.str());
    VStringBuffer checkJobExitCode("kubectl get pods --selector=job-name=%s --output=jsonpath={.items[*].status.containerStatuses[?(@.name==\"%s\")].state.terminated.exitCode}", jobName.str(), jobName.str());

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
            if (!streq(output, "1"))  // status.active value
            {
                // Job is no longer active - we can terminate
                DBGLOG("kubectl jobs output: %s", output.str());
                runKubectlCommand(componentName, checkJobExitCode, nullptr, &output.clear());
                if (output.length() && !streq(output, "0"))  // state.terminated.exitCode
                    throw makeStringExceptionV(0, "Failed to run %s: pod exited with error: %s", jobName.str(), output.str());
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
        PROGLOG("The podName = %s", myPodName.str());
    };
    if (isContainerized())
        podInfoInitCBId = installConfigUpdateHook(updateFunc, true);
    return true;
}
MODULE_EXIT()
{
    removeConfigUpdateHook(podInfoInitCBId);
}

#else

const char *queryMyPodName()
{
    throwUnexpected();
}

KeepJobs translateKeepJobs(const char *keepJobs)
{
    throwUnexpected();
}

bool isActiveService(const char *serviceName)
{
    throwUnexpected();
}

void deleteResource(const char *componentName, const char *job, const char *resource)
{
    throwUnexpected();
}

void waitJob(const char *componentName, const char *resourceType, const char *job, unsigned pendingTimeoutSecs, KeepJobs keepJob)
{
    throwUnexpected();
}

bool applyYaml(const char *componentName, const char *wuid, const char *job, const char *resourceType, const std::list<std::pair<std::string, std::string>> &extraParams, bool optional, bool autoCleanup)
{
    throwUnexpected();
}

void runJob(const char *componentName, const char *wuid, const char *job, const std::list<std::pair<std::string, std::string>> &extraParams)
{
    throwUnexpected();
}

std::vector<std::vector<std::string>> getPodNodes(const char *selector)
{
    throwUnexpected();
}

#endif // _CONTAINERIZED

} // end of k8s namespace
