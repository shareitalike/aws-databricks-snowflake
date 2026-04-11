# 🛡️ Failure Handling & Retry Mechanisms (AWS Kinesis + Lambda)

One of the most common senior-level interview questions is: *"What happens to your data if the Lambda function crashes?"* 

Our project just experienced this in real life (with the `ImportModuleError`). Here is exactly how the system prevented data loss.

---

## 1. Where was the data during the failure?
When our Lambda failed to start, the data didn't just "disappear." It was sitting safely in the **AWS Kinesis Shards**.

*   **The Buffer:** Kinesis acts like a "Wait Room." By default, it stores every record for **24 hours**. 
*   **The Cursor (Iterator):** AWS Lambda keeps a "bookmark" called a **Sequence Number**. Even if the code crashes 100 times, the bookmark stays at the last successful record. It won't move forward until the Lambda successfully processes the batch.

---

## 2. The Retry Mechanism (Kinesis ➡️ Lambda)
When the "Security Guard" (Lambda) failed, here is what AWS did behind the scenes:

1.  **Detection:** Lambda reported an `ImportModuleError`.
2.  **Retry:** Because the data is coming from a stream (Kinesis), AWS **automatically retries** the same batch of 100 records over and over again.
3.  **Backoff:** It doesn't just spam the Lambda; it waits a few milliseconds/seconds between retries.
4.  **Exponential Retries:** It will keep trying until:
    *   The code is fixed (which we did!).
    *   **OR** the data reaches the "Retention Limit" (24 hours) and expires.

---

## 3. How we prevented Duplicates (Idempotency)
A major risk of retries is **Duplicate Data**. If Lambda successfully writes to S3 but then crashes *before* telling Kinesis "I'm done," Kinesis will send the same data again.

We handle this in two ways:

### A. UUID Filenames (In Lambda)
In `transform_handler.py`, we generate a unique ID for every file:
```python
filename = f"{uuid.uuid4().hex}.json"
```
Even if the same batch is retried, it creates a **new file** rather than overwriting an old one. This ensures we never "lose" data by overwriting it.

### B. Deterministic Deduplication (In Databricks)
In our Spark code, we don't just "trust" the data. We use:
```python
Window.partitionBy("event_id").orderBy("ingestion_timestamp")
```
This ensures that if 5 copies of the same event arrived due to retries, Spark will only keep the **very first one**.

---

## 4. Key Interview Terms for You
*   **At-Least-Once Delivery:** Kinesis guarantees the data will arrive at least once, but maybe more.
*   **Idempotency:** The ability to run the same operation multiple times and get the same result (handled by our Spark dedup).
*   **Poison Pill:** A record that is so "bad" it makes the Lambda crash every time. (Advanced: we would use a **Dead Letter Queue (DLQ)** or **On-failure destination** to handle these).

---

> [!TIP]
> **Story for your Portfolio:** 
> "During development, I encountered a Cold Start error where the Lambda handler was misconfigured. Because I used Kinesis as a buffer, no data was lost; the records stayed in the shards until I corrected the configuration, at which point Lambda automatically resumed and processed the backlog."
