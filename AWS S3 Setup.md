# **AWS S3 Setup Guide**

# **Introduction to AWS**
Amazon Web Services (AWS) is a comprehensive cloud platform offering various services for building scalable and reliable applications. Among its core services, storage solutions play a crucial role in modern cloud architecture.

# **Understanding AWS Storage Services**
AWS provides several storage options:
- **S3 (Simple Storage Service)**: Object storage service with industry-leading scalability and durability
- **S3 Glacier**: Low-cost storage for data archiving
- **EBS (Elastic Block Store)**: Block storage for EC2 instances
- **EFS (Elastic File System)**: Managed file storage for EC2 instances

This project focuses on S3, utilizing the AWS Free Tier which includes:
- 5GB of standard storage
- 20,000 GET requests
- 2,000 PUT, COPY, POST, or LIST requests
- Data transfer up to 100GB per month

# **Creating an S3 Bucket**
## **Basic Configuration**
1. Navigate to AWS Console > S3
2. Click "Create bucket"
3. Enter bucket name: `windiwwd_projects`

## **Object Ownership**
S3 offers two ownership options:

  1. **ACLs disabled (Recommended)** - Selected for this project
   - All objects are owned by the bucket owner
   - Access control is managed solely through bucket policies
   - Simplifies permissions management

  2. **ACLs enabled**
   - Objects can be owned by other AWS accounts
   - Access can be controlled through ACLs
   - More complex but offers granular control

## **Public Access Settings**
For this project, we've disabled the "Block all public access" setting. While AWS recommends blocking public access for security, our decision is based on:
  - Need for public access to specific objects
  - Controlled access through bucket policies
  - Implementation of specific security measures at the object level

The four public access settings explained:
  1. **Block new ACLs**: Prevents new public access through ACLs
  2. **Block any ACLs**: Ignores all ACLs granting public access
  3. **Block new policies**: Prevents new public access through bucket policies
  4. **Block any policies**: Ignores all policies granting public access

## **Bucket Versioning**
Versioning options:
  - **Enabled**: Maintains multiple versions of objects for recovery and protection
  - **Disabled**: Only keeps current version (Selected for this project)

We chose to disable versioning to:
  - Minimize storage costs
  - Simplify object management
  - Focus on demonstration purposes

## **Encryption Configuration**
Selected: Server-side encryption with Amazon S3 managed keys (SSE-S3)
  - Provides automatic encryption at rest
  - AWS manages encryption keys
  - No additional cost
  - Sufficient security for our use case

## **Bucket Key**
Enabled bucket key to:
  - Reduce costs associated with AWS KMS requests
  - Minimize API calls to AWS KMS
  - Improve performance

## **Advanced Settings**
Object Lock: Disabled
  - Not required for our demonstration purposes
  - Simplifies bucket management
  - Suitable for non-critical data

![Gambar 1](https://drive.google.com/uc?export=view&id=19QPWTRdqmfFihKOap4LaJapVYmqvXJKW)

# **AWS S3 Bucket Policy Configuration**

## **Bucket Policy Setup**
After creating the bucket, we need to configure the bucket policy to manage access to the S3 bucket. Here are the steps:

1. Navigate to the "Permissions" tab on the bucket
2. Scroll to the "Bucket policy" section
3. Click "Edit" to input the policy JSON

## **Bucket Policy Explanation**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::windiwwd-projects",
                "arn:aws:s3:::windiwwd-projects/*"
            ]
        }
    ]
}
```

## **Policy Component Analysis**
  1. **Version**: "2012-10-17"
   - Standard version of AWS policy language
   - Latest version supporting all policy features

  2. **Statement**: Main block defining one or more permissions
   
  3. **Effect: "Allow"**
   - Permits access for defined actions
   - Alternative is "Deny" to block access

  4. **Principal: "*"**
   - Allows access from all users/services
   - Used to enable Databricks integration
   - Should be balanced with proper access controls at object level

  5. **Action**: Defines allowed operations
   - `s3:GetObject`: Retrieve/read objects from bucket
   - `s3:ListBucket`: View list of objects in bucket
   - `s3:PutObject`: Upload objects to bucket

  6. **Resource**: Determines policy scope
   - `arn:aws:s3:::windiwwd-projects`: Refers to the bucket itself
   - `arn:aws:s3:::windiwwd-projects/*`: Refers to all objects within the bucket

## **Configuration Purpose**
This policy is configured to:
1. Enable Databricks Integration
   - Databricks needs access to read and write data
   - List capability required for data browsing

2. Provide Operational Flexibility
   - Direct data upload to bucket (PutObject)
   - Data reading for analysis (GetObject)
   - Bucket content viewing (ListBucket)

## **Security Considerations**
While this policy provides relatively open access, several security practices are implemented:
1. Access limited to specific bucket
2. Only necessary operations allowed
3. No permissions for object deletion
4. No permissions for bucket configuration modification


## **Uploading Data to AWS S3**

### **Data Upload Process**
  1. Navigate to the `windiwwd-projects` bucket
  2. Click the "Upload" button on the left panel
  3. Drag and drop the CSV file or click "Add files" to select file
  4. Review default upload settings:
   - Storage class: Standard
   - Encryption: Using bucket default configuration (SSE-S3)
  5. Click "Upload" to start the process

## **Upload Verification**
After upload completion, you will see:
  - CSV file in the bucket objects list
  - Information such as:
    - File size
    - Upload date
    - Storage class
    - Encryption status

![Gambar 2](https://drive.google.com/uc?export=view&id=1DfjYMDHSZ_ydV8bcZCXnSBNaDbeulYgT)

## **Next Steps**
With the data now stored in AWS S3, we will proceed to Databricks setup and configuration to begin the data analysis process.