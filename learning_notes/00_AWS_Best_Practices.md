# 🔐 AWS Setup Best Practices & Account Management

## 1. Finding Your AWS Account ID
Your AWS Account ID is a 12-digit number that uniquely identifies your AWS account. You'll need this for almost every AWS command and configuration.

### How to find it:
1.  **Via AWS Console:** 
    *   Log in to the AWS Management Console.
    *   Click on your **Username/Account Name** in the top right corner.
    *   The 12-digit **Account ID** is shown in the dropdown menu.
2.  **Via CLI (if already configured):**
    *   Run: `aws sts get-caller-identity --query Account --output text`

---

## 2. Choosing an S3 Bucket Name
S3 bucket names must be **globally unique** across all of AWS (not just your account).

### Rules for naming:
*   **Be Unique:** If someone else already has a bucket named `my-data-bucket`, you cannot use it. 
*   **Recommended Pattern:** `[your-name]-[project-name]-[environment]-[date]`
    *   *Example:* `rahul-event-analytics-dev-2026`
*   **Small Letters only:** Use lowercase letters, numbers, and hyphens.

---

## 3. Should I use the "Root User"? (CRITICAL)

### **The Short Answer: NO.** 🛑

### **The Explanation:**
The "Root User" is the email address you used to sign up for AWS. It has ultimate power over your billing and all resources. If these credentials are stolen or leaked (e.g., if you accidentally push them to GitHub), someone could delete your whole account or run up a $50,000 bill in an hour.

### **The "Data Engineer" Way (Best Practice):**
You should create an **IAM User** (Identity and Access Management) and use that for your daily work.

1.  Log in as Root.
2.  Go to the **IAM Console**.
3.  Create a new user (e.g., `developer-admin`).
4.  Give this user the `AdministratorAccess` policy (for learning/testing) or specific Kinesis/S3/Lambda permissions.
5.  Generate **Access Keys** for this IAM user.
6.  Run `aws configure` on your laptop using **ONLY** the IAM User keys, never the Root keys.

---

## 4. Updating your `config.yaml`
Once you have your Account ID and chosen a bucket name, update `f:\pyspark_study\project2_aws\aws-databricks-snowflake-pipeline\config\config.yaml`:

```yaml
aws:
  region: "ap-south-1"  # Or your closest region
  account_id: "123456789012"  # Your 12-digit ID

s3:
  bucket_name: "rahul-event-analytics-2026"  # Your unique name
```

> [!IMPORTANT]
> **NEVER** share your `config.yaml` or any files containing your Account ID or Access Keys on public forums or GitHub. This project has `.gitignore` configured to help keep you safe.
