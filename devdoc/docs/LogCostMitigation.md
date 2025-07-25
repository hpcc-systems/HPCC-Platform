As part of our ongoing initiative to optimize system performance and reduce operational costs,
we are focusing on minimizing excessive and verbose log output across our components.
Logging is essential for observability and debugging, but uncontrolled log volume has led to
increased storage costs, slower processing, and reduced clarity in identifying critical issues.
The following tasks are designed to guide developers in refining logging practices—ensuring that
logs remain meaningful, efficient, and cost-effective without compromising visibility into system behavior.

- Scrutinize the log’s purpose and impact.
    - Determine what actions can be taken based on the message. 
    - If the message doesn’t lead to a useful action, consider removing it.
- Reduce the frequency of the message. Hoist it outside of loops, or track the count of instances and report periodically.

    Example:

    Before:
    ```c++
    ...
    while (someCondition)
    {
        LOG(PROG, "Detected some condition");
        someCondition = performSomeLogic();
    }
    ```

    After:
    ```c++
    int conditionCounter = 0;
    while (someCondition)
    {
        conditionCounter++;
        someCondition = performSomeLogic();
    }
    if (conditionCounter > 0)
        LOG(PROG, "Detected some condition %d times", conditionCounter);
    ```

- Condense the message. Use direct language, remove redundant annotations and unnecessary delimiters. Utilize error/warning codes.

    Example:

    Before:
    ```c++
    LOG("------\nSomething important happened in mycomponent, please contact your system administrator\n------");
    ```
    After:
    ```c++
    LOG("mycomponent: Something important happened");
    ```

- Summarize multiple related messages into a single entry.

    Example:

    Before:
    ```c++
    LOG("Received a request");
    LOG("Request First line: '%s'", request->firstLine());
    LOG("Request is of type: '%s', will parse headers", request->type());
    LOG("Request headers: '%s'", request->headers());
    LOG("Finished processing request");
    ```
    After:
    ```c++
    LOG("Processed request - First Line: '%s', type: '%s', headers: '%s'", request->firstLine(), request->type(), request->headers());
    ```

- Recategorize messages to accurately reflect their importance and target audience.

    Example:

    Before:
    ```c++
    LOG(DEBUG, “Something important happened!”); //if logDetail is set low, this important message would be ignored, which prevents admins from lowering the logDetail level
    ```
    After:
    ```c++
    LOG(WARN, “Something important happened!”); //Message reported even if logDetail is lower than “Debug”
    ```

- Use feature-level trace flags to control the reporting of log messages. Allow users/admins to choose the types of messages reported.

    Example:

    Before:
    ```c++
    LOG(INFO, “XYZ feature specific information”); //Message reported even if admin/users not interested in XYZ feature specific information
    ```
    After:
    ```c++
    if (doTrace(traceXYZDetails)) // Only log if admin/users interested in feature details
    {
        LOG(INFO, “XYZ feature specific information”); //Message reported only if tracing for XYZ feature is enabled
    }
    ```

- Control the reporting of messages based on relevant variables (e.g., timings, conditions, etc.).
    
    Example:

    Before:
    ```c++
    processTransaction(const Transaction& tx);
    LOG(INFO, “Transaction ‘%d’ completed in ‘%dms”, tx.id, tx.duration); //Transaction completion always reported
    ```
    After:
    ```c++
    // Threshold in milliseconds for reporting slow transactions
    const int slowTransactionThresholdMs = getConfigValue(SLOW_TRANSACTION_THRESHOLD_MS, 500);
    processTransaction(const Transaction& tx);
    if (tx.duration >= slowTransactionThresholdMs)
        LOG(WARN, “Transaction ‘%d’ completed in above expected time: ‘%dms”, tx.id, tx.duration); //Transaction completion only reported if thresholds are exceeded
    ```

- Consider sampling non-essential log entries (e.g., log only a percentage of repetitive events).
    
    Example:

    Before:
    ```c++
    LOG(INFO, “some non-essential information”); //Non-essential information always logged
    ```
    After:
    ```c++
    // Probability of logging a message (e.g., 0.1 = 10%)
    const double samplingRate = getConfigValue(LOG_SAMPLE_RATE, 0.1);
    // Random number generator setup
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);

    if (dis(gen) < samplingRate)
        LOG(INFO, “some non-essential information”); //Non-essential information only logged according to samplingRate
    ```

- Offload detailed messages to post-mortem or diagnostic features.
   //todo

- Utilize the metrics framework to report quantitative performance or resource related values.
  
    Example:

    Before:
    ```c++
    LOG(INFO, ("X Resource Utilization: Total Available=%u, Total Used=%u “, resource->available, resource->used)); //Logs resource values periodically
    ```
    After:
    ```c++
    std::shared_ptr<Metric> xResourceAvailable = std::make_shared<Metric>("AvailableXResource", "Available X Resource at a given time", SMeasureCount);
    std::shared_ptr<Metric> xResourceUsed = std::make_shared<Metric>("UsedXResource", "Utilized X Resource at a given time", SMeasureCount);

    xResourceAvailable->set(resource->available); //periodically report X resource metric
    xResourceUsed->set(resource->used); //periodically report X resource metric
                                        //Metrics are annotated and distributed to observability applications
    ```

- Utilize JTrace to report transaction-specific information, particularly timings and remote calls.

    Example:

    Before:
    ```c++
    {
        performUserAuthorization(req->ctx);
        LOG(INFO, "Transaction id=%s authorization result=%s time to complete=%u", req->id, req->ctx->auth->result, req->ctx->auth->time); //As transaction steps are completed, timing and result is logged
    }
    ```
    After:
    ```c++
    OwnedActiveSpanScope requestSpan = queryTraceManager().createServerSpan("run_workunit", traceHeaders);
    …
    {
        OwnedActiveSpanScope clientAuthSpan = requestSpan ->createClientSpan("userAuth"); // User Auth span declared and associated with current trace
        bool authResult = performUserAuthorization(req->ctx);
        clientAuthSpan ->setSpanAttribute(“authResult”, authResult); //annotating client AuthSpan with auth request result
    } // start/end time attached to this span

- Migrate functional values from logs to queryable services such as wsstore.

    Example:

    Before:
    ```c++
    LOG(INFO, “Interesting component values: userID=%s, sessionID=%s, timesSessionExtended=%u”, userID, sessionID, sessionExtendedCount); //interesting values reported to log
    ```
    After:
    ```c++
    componentStore->setValue(userID,sessionID,timesSessionExtended); // report interesting values to a service which 
                                                                     // exposes the values through a service
    ```
