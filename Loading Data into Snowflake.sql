--------------------------------------------------------------------------------------
-- Loading Data into Snowflake tables
--------------------------------------------------------------------------------------
/*
Loading data is a process of bringing data from external sources to snowflake table
Snowflake supports 2 types of data loading
1. Bulk Loading / Batch Loading: Data is collected over time → then loaded in bulk at periodic intervals
2. Continuos Loading: Loading data into Snowflake automatically and near real-time as it arrives.
*/

---------------------------------------------------------------------------------------
-- Bulk loading
---------------------------------------------------------------------------------------
/*
- Data is collected over time → then loaded in bulk at periodic intervals
  Example: Every hour, day, or once a night (ETL job).
- It supports multiple file formats like (JSON, AVRO, ORC, PARQUET, XML, CSV)
- Bulk Loading is done via stages
*/

------------------------------------------
-- Stages
------------------------------------------
/*
    - For bulk loading we cannot directly load the data from external sources to snowflake tables, instead we,
        - Upload external files to a stage
        - Load from stage to table
    - There are 2 types of stages
        1. Internal stage: used to load data from local system
        2. External stage: used to load data from cloud storage
*/

------------------------------------------------------------------------------------
-- 1. Internal stages: These are used to load data from local system to stage are
------------------------------------------------------------------------------------
/*
3 types of internal stages:
1. User stage (@~)
2. Table stage (%tablename)
3. Named stage
*/

--------------------------------------------------
-- User stage (@~)
--------------------------------------------------

/*
- Every snowflake user by default gets there own user stage.
- It acts like that user’s personal cloud storage space inside Snowflake — where they can upload,store files without needing to create a separate named stage
- You refer to a user stage using @~
*/

-- Put the files from local system to user stage (Syntax: PUT filename @~)
PUT 'file://D://Complete Learning//Snowflake//data/employees.csv' @~;           



-- List all the files on user stage
list @~;
-- By default, PUT compresses local files to gzip, this:
✅ Reduce upload time (smaller file size)
✅ Reduce storage cost in Snowflake
✅ Improve load speed during COPY INTO



-- if file with same name already exists in user stage then, by default PUT does not overwrite the file. make OVERWRITE = TRUE
PUT 'file://D:/Complete Learning/Snowflake/data/employees.csv' @~ 
OVERWRITE = TRUE;
list @~;



-- If you do not want compression, you can use AUTO_COMPRESS = FALSE:
PUT 'file://D:/Complete Learning/Snowflake/data/employees.csv' @~ 
OVERWRITE = TRUE 
AUTO_COMPRESS = FALSE;
list @~;



-- To remove a file from user stage
REMOVE @~/employees.csv;


-- To download files from stage to local storage (snowsql)
GET @~/employees.csv 'file://D://Complete Learning//Snowflake//data';

-- Note: PUT,GET will not work on snowsight. you have to run them from snowsql


-------------------------------------------------------
-- Table stage (@%tablename)
-------------------------------------------------------
/*
- Every snowflake table by default gets there own table stage
- It’s used to temporarily store files that you plan to load/unload
- You refer to a table stage using @%tablename
*/

-- Put the files from local system to table stage (Syntax: PUT filename @%tablename)

use testdb.public;

CREATE OR REPLACE TABLE employees (
    Id INT,
    Name STRING,
    Gender STRING,
    Salary NUMBER(10,2),
    Country STRING
);

PUT 'file://D://Complete Learning//Snowflake//data/employees.csv' @%employees;           


-- list files in tablestage
list @%employees;



-- To remove a file from table stage
REMOVE @%employees/employees.csv;
list @%employees;


-- To download files from stage to local storage (snowsql)
GET @%employees/employees.csv 'file://D://Complete Learning//Snowflake//data';

--------------------------------------------------------------------------------
-- Named Internal stage (@stagename)
--------------------------------------------------------------------------------
/*
- This stage is a storage location that you create and control
- The storage area is inside snowflake's cloud storage (snowflake managed storage area)
- User and table stage are tied to a specific user or table, whereas Named stage can be used by multiple users,roles or tables.
- You refer to a named stage using @stagename
*/

create or replace database manage_db;
create schema staging_assets;
use manage_db.staging_assets;

-- Create Named Stage
CREATE STAGE stage_internal_csv;



-- Show all stages in 
show stages;



-- Create stage with file-format
CREATE or REPLACE STAGE stage_internal_csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"');



-- To view metadata (storage type, file format, etc.):
DESC STAGE stage_internal_csv;



-- PUT data to stage
PUT 'file://D://Complete Learning//Snowflake//data/employees.csv' @stage_internal_csv;


-- List Files in Named Stage
list @stage_internal_csv;



-- Remove files
REMOVE @stage_internal_csv/employees.csv;



-- Download file from stage to local storage (snowsql)
GET @stage_internal_csv/employees.csv 'file://D://Complete Learning//Snowflake//data';


-- Drop stage
DROP STAGE stage_internal_csv;


------------------------------------------------------------------------------------
-- 2. External stages:
------------------------------------------------------------------------------------
/*
- Its a named object that references an external cloud storage location (like AWS S3, Azure Blob, or Google Cloud Storage) so that you can read from or write to that location directly from Snowflake.
- Unlike internal stages, Snowflake does not store files itself here. Instead, it stores the connection details and metadata required to access the files in your cloud storage.
*/

-------------------------------------------------------------
-- Create a Stage that can access public s3 buckets:
-------------------------------------------------------------

create or replace stage manage_db.staging_assets.stage_external_aws
    url = 's3://snowflake-data-naziraa/csv/';
show stages;


-- View metadata (storage type, file format, etc.):
DESC STAGE stage_external_aws;


list @stage_external_aws;

/*
Note: The S3 bucket should be public. 

**To make a S3 bucket public**

1. Click on the bucket → Permissions Tab → Block public access (bucket settings) Edit → 
    Uncheck (Block all public access) → Save Changes
2. Edit bucket policy that explicitly allows public access to the files:

    Permissions Tab → Bucket Policy → Edit → Paste the below Policy  → Save Changes
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Sid": "Statement1",
          "Effect": "Allow",
          "Principal": "*",
          "Action": "s3:*",
          "Resource": "arn:aws:s3:::snowflake-data-naziraa"
        }
      ]
    }
    You can get the above policy generated automatically from AWS Policy Generator which is available in the “Edit Bucket Policy Option” 
    
    Click Policy Generator → Type of Policy : S3 Bucket Policy → Effect : Allow → Principal : * → 
    Actions : Check all actions → ARN :   arn:aws:s3:::snowflake-data-naziraa (You can get this from properties tab) → Add Statement → Generate Policy
*/

---------------------------------------------------------
-- Create a Stage that can access private s3 bucket
---------------------------------------------------------
/*
For this you have to create IAM user with AmazonS3FullAccess Policy attached and get the access key credentials for this IAM user

1. AWS Console → IAM → Users → Create user → Enter a username (e.g. `snowflake-user`) → Next 
    → Attach Policies Directly → AmazonS3FullAcess → Next → Create User
2. Click on the created User → Create Acess Key → Application running outside AWS
    📌 Important: Save them safely — you won’t be able to view the secret key again!
        Acess Key: AKIA54UKYHR4TCEXLV5P 
        secret access key : XI/dqYbZXQQido+Emo/vCNc1zcWY1LqlivICe7sd
*/

CREATE OR REPLACE STAGE manage_db.staging_assets.stage_external_aws_snowflakedatanaziraa
  URL = 's3://snowflake-data-naziraa/csv/'
  CREDENTIALS = (
    AWS_KEY_ID = 'AKIA54UKYHR4TCEXLV5P'
    AWS_SECRET_KEY = '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'
  );

list @manage_db.staging_assets.stage_external_aws_snowflakedatanaziraa;

/*
The above IAM user (snowflake-user) has FullAccess to complete s3. 
You can also create a IAM user with FullAccess to a specific bucket

1. Create IAM user and policy, Attach policy to the user:

    AWS Console → IAM → Create user → Enter a user name (e.g., `snowflake-user1`) → Next → 
    Create policy (opens in new tab) → Visual editor tab → Service**: `S3` → Actions: Click All S3 actions → Resources → 
        
        - Bucket: Add ARN →  Enter your bucket name (e.g., `snowflake-data-naziraa`) → Click Add
        - Object: Add ARN → Bucket: `snowflake-data-naziraa` → 
                  Prefix: (leave blank for full access or set `csv/` to limit to a folder) → Click Add → Next →
                  name it something like `s3_snowflake-data-nazira_fullaccess` → Create policy
    
    Now Attach That Policy to Your IAM User: Go back to the Create User tab → Refresh policy list → Search for `s3_snowflake-data-nazira_fullaccess` → Select it → Next → Create user

2. Create Access Keys

Click on the created User → Create Acess Key → Application running outside AWS
📌 Important: Save them safely — you won’t be able to view the secret key again!

Access Key= 'AKIA54UKYHR4XV2DAPHM'
Secret Access Key = 'WE2wDWKGsHDoBFCJGpJirmaOyMHhbIlGrxoICNfa'
*/


CREATE OR REPLACE STAGE manage_db.staging_assets.stage_external_aws_snowflakedatanaziraa
  URL = 's3://snowflake-data-naziraa/csv/'
  CREDENTIALS = (
    AWS_KEY_ID = 'AKIA54UKYHR4XV2DAPHM'
    AWS_SECRET_KEY = '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'
  );

list @manage_db.staging_assets.stage_external_aws_snowflakedatanaziraa;


-------------------------------------------------------------------
-- Storage integration
-------------------------------------------------------------------
/*
- Using IAM User Access Keys in Snowflake Stages (Not Recommended)

🔹 What is an IAM User?
- Represents a person or application needing long-term access to AWS.
- Uses username/password (for AWS Console) and/or access keys (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY) for programmatic access.
- Credentials do not expire automatically — they must be rotated manually.

- Problems with this approach
    1. Long-lived credentials, no automatic expiration:
        These access keys don’t expire automatically.
        If someone gets hold of them — for example, through logs, screenshots, or code leaks — they can directly access your S3 bucket
    2. Stored in Snowflake metadata:
        Even though Snowflake encrypts these keys, they still reside inside its system metadata.
        This means account administrators or users with high-level privileges might still have access to them, creating a potential misuse risk.
    3. Manual management:
        Whenever you rotate or change your access keys in AWS, you’ll have to manually update every stage in Snowflake that uses those  
        credentials.
        This becomes tedious and error-prone when multiple stages are involved.
    4. Compliance risk:
        Storing permanent credentials within a system like Snowflake goes against most security and compliance standards,
        such as SOC 2, ISO 27001, and CIS Benchmarks.
        

- Instead of using an IAM user with static credentials, use a storage integration that assumes IAM Role
🔹 IAM Role
- Does not belong to a specific person or app.
- It’s like a set of permissions that any trusted entity can “temporarily assume.”
- When assumed, AWS gives temporary security credentials (auto-expire).

- A Storage Integration is a Snowflake object that securely connects to cloud storage (like S3) without using access keys.
- Instead of storing credentials, Snowflake assumes an IAM role you set up in AWS, using a secure external ID, allowing safe, temporary access to your bucket.

- Benefits:

    1️. No more long-lived credentials:
    Snowflake doesn’t need your AWS_KEY_ID and AWS_SECRET_KEY.
    Instead, it uses temporary tokens generated by AWS STS (Security Token Service).
    These tokens automatically expire after a short time, so even if someone somehow gets one, it quickly becomes useless.
    
    2. No sensitive data stored in Snowflake
    Because Snowflake only keeps the reference to the integration (not the actual AWS keys),
    your credentials never get stored in its metadata.
    That means no risk of exposure through system tables or logs.
    
    3️. Automatic token refresh
    Snowflake automatically fetches new temporary tokens whenever needed —
    you don’t have to rotate or manually update anything.
    This eliminates the burden of credential management.
    
    4️. Fully compliant and enterprise-grade
    Since there are no static keys, and access is based on role assumption,
    it aligns with enterprise security standards like SOC 2, ISO 27001, and CIS.
    Many companies are required to use this approach to stay compliant.
*/
---------------------------------------------------------------------------
-- Create IAM ROle in AWS
---------------------------------------------------------------------------
/*
AWS Console -> IAM -> Roles -> Create Role -> AWS Account -> Account ID , Check Require External ID (for now you can set it to any temp value : 00000) -> Next -> Permission Policies : AmazonS3FullAccess -> Next -> RoleName (ex. snowflake-access-role), Description -> Create Role
*/

-----------------------------------------------
-- Create Storage Integration
-----------------------------------------------


create or replace storage integration si_aws_s3
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::954847476857:role/snowflake-access-role'      -- You can get this from aws role you created
    STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-data-naziraa/csv/')
    STORAGE_BLOCKED_LOCATIONS = ()
    COMMENT = 'This is storage integration object for aws s3 bucket';

/*
Note: STORAGE_BLOCKED_LOCATIONS takes preceedance over STORAGE_ALLOWED_LOCATIONS: if same path is mentioned in both then it will be considered in blocked locations
*/

-- Show storage integrations
SHOW INTEGRATIONS;

desc integration si_aws_s3;

-- Note: STORAGE_AWS_IAM_USER_ARN, STORAGE_AWS_EXTERNAL_ID 

-- Update them in trust relationship of aws role you created
-- AWS Console -> IAM -> Roles -> created role (snowflake-access-role) -> Trust relationship -> Edit
/*
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Principal": {
                "AWS": "954847476857"             <--------- Update this with STORAGE_AWS_IAM_USER_ARN
            },
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "00000"     <--------- Update this with STORAGE_AWS_EXTERNAL_ID
                }
            }
        }
    ]
}
*/

---------------------------------------------------
-- Create Stage Object with Storage Integartion
---------------------------------------------------
CREATE OR REPLACE STAGE manage_db.staging_assets.stage_external_aws_snowflakedatanaziraa
  URL = 's3://snowflake-data-naziraa/csv/'
  STORAGE_INTEGRATION = si_aws_s3
  FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

list @stage_external_aws_snowflakedatanaziraa;

----------------------------------------------
-- Alter Storage Integration
-----------------------------------------------

Alter Storage Integration si_aws_s3 set STORAGE_ALLOWED_LOCATIONS = ('s3://Location1',
                                                                    's3://Location2');

desc storage integration si_aws_s3;

CREATE OR REPLACE STAGE manage_db.staging_assets.stage_external_aws_snowflakedatanaziraa
  URL = 's3://snowflake-data-naziraa/csv/'
  STORAGE_INTEGRATION = si_aws_s3
  FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);


Alter Storage Integration si_aws_s3 set STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-data-naziraa/csv/');

desc storage integration si_aws_s3;

CREATE OR REPLACE STAGE manage_db.staging_assets.stage_external_aws_snowflakedatanaziraa
  URL = 's3://snowflake-data-naziraa/csv/'
  STORAGE_INTEGRATION = si_aws_s3
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER=1);

list @stage_external_aws_snowflakedatanaziraa;


-------------------------------------------------
-- File Format
-------------------------------------------------

/*
- A snowflake file format is a snowflake database object
- File format defines how data files (CSV,jSON,PARQUET etc.) are structured so snowflake knows how to read or write them during data loading/unloading 
- You can reuse a file format objects
*/

Create or Replace File Format csv_file_format
TYPE = CSV,
SKIP_HEADER = 1;

DESC FILE FORMAT csv_file_format;

CREATE OR REPLACE STAGE manage_db.staging_assets.stage_external_aws_snowflakedatanaziraa
  URL = 's3://snowflake-data-naziraa/csv/'
  STORAGE_INTEGRATION = si_aws_s3
  FILE_FORMAT = (FORMAT_NAME = 'csv_file_format');

list @stage_external_aws_snowflakedatanaziraa;


-- file format object have different parameters depending on the type of the file

-- csv
/*
| Parameter                    | Description                             | Example          |  
| -----------------------------| --------------------------------------- | ---------------- |
| TYPE                         | File type                               | 'CSV'            |   
| FIELD_DELIMITER              | Character that separates fields         | ',', '\t'        |
| SKIP_HEADER                  | Number of header rows to skip           | 1                |    
| FIELD_OPTIONALLY_ENCLOSED_BY | Quote character for enclosing fields    | '"'              |    
| ESCAPE                       | Escape character for special characters | '\\'             |    
| NULL_IF                      | Treat these values as NULL              | ('NULL', 'null') |    

*/
CREATE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL', 'null');

-- json
/*
| Parameter          | Description                           | Example|
| -------------------| ------------------------------------- | -------|
| TYPE               | File type                             | 'JSON' |
| STRIP_OUTER_ARRAY  | If true, removes outer array brackets | TRUE   |
| IGNORE_NULL_VALUES | If true, skips null fields            | FALSE  |
| ALLOW_DUPLICATE    | Allow duplicate keys in JSON objects  | TRUE   |

*/
CREATE FILE FORMAT my_json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE
  IGNORE_NULL_VALUES = FALSE,
  ALLOW_DUPLICATE = FALSE;

  -- PARQUET
  /*
| Parameter      | Description                    | Example                        |
| ---------------| ------------------------------ | ------------------------------ |
| TYPE           | File type                      | 'PARQUET'                      |
| COMPRESSION    | Compression algorithm used     | 'SNAPPY', 'GZIP', 'NONE'       |
| BINARY_AS_TEXT | Convert binary columns to text |  FALSE                         |
| ENABLE_OCTAL   | Allow octal number parsing     |  FALSE                         |

*/

CREATE FILE FORMAT my_parquet_format
  TYPE = 'PARQUET'
  COMPRESSION = 'SNAPPY';

-- AVRO File Format

/*
| Parameter     | Description      | Example                           |
| ------------- | ---------------- | --------------------------------- |
| TYPE          | File type        | 'AVRO'                            |
| COMPRESSION   | Compression used | 'DEFLATE', 'SNAPPY', 'NONE'       |
*/

CREATE FILE FORMAT my_avro_format
  TYPE = 'AVRO'
  COMPRESSION = 'SNAPPY';

-- ORC File Format

/*
| Parameter  | Description                      | Example |
| -----------| -------------------------------- | ------- |
| TYPE       | File type                        |  'ORC'  |
| TRIM_SPACE | Remove spaces from string values |  TRUE   |
*/

CREATE FILE FORMAT my_orc_format
  TYPE = 'ORC'
  TRIM_SPACE = TRUE;

-- XML File Format

/*
| Parameter           | Description                  | Example |
| --------------------| ---------------------------- | ------  |
| TYPE                | File type                    | 'XML'   |
| IGNORE_NULL_VALUES  | Ignore NULLs in XML          | TRUE    |
| STRIP_OUTER_ELEMENT | Remove outermost XML element | TRUE    |
| PRESERVE_SPACE      | Keep white space             | FALSE   |

*/

CREATE FILE FORMAT my_xml_format
  TYPE = 'XML'
  STRIP_OUTER_ELEMENT = TRUE
  IGNORE_NULL_VALUES = TRUE;