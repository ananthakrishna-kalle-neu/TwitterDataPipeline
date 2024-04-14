# _**Twitter ETL Pipeline using Airflow and AWS**_

## **Project Summary**

![10](https://github.com/ananthakrishna-kalle-neu/TwitterDataPipeline/assets/114686559/6f7d851a-5b5a-4349-93a6-3ef3fad8d57f)

a) **Data Collection phase**: Tweets associated with a Twitter account were extracted using Python and the 'snscrape' package.

b) **Apache Airflow Setup phase**: An EC2 instance was set up on Amazon Web Services. Apache Airflow was installed on the EC2 instance. The necessary packages were downloaded during the setup.

c) **Airflow DAG Creation phase**: An Airflow DAG (Directed Acyclic Graph) was developed to outline the workflow for extracting and storing tweets in S3.Tasks were created for extracting tweets, transforming them into a suitable format for storage in S3, and uploading them to S3.

d) **Data Storage phase**: An S3 bucket was created on Amazon Web Services for storing the extracted tweets.The necessary permissions were configured to enable data storage in the S3 bucket.
