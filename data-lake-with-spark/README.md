# Project: Data Lake
## Company: Sparkify
### By: Salinee Kingbaisomboon

<hr />

### Purpose of this database:
The purpose of database is to keep data systematic so that it can be easily managed, accessed and update. We decide to to move their data warehouse to a data lake as the business has grow their user base and song database. This will ease the analytics's purposes on the big data.

### Analytical goals:
  - **Customer-Facing team (such as marketing):** marketing team can use this database to generate personalized communications in order to promote a product or service for marketing purposes.
  - **Executive team (such as CEO):** They need to use data to run and grow their business. The goal is to help them make better decisions, which can take them to the top of their market.
  - **Data Scientist team:** the main goal of this team is to extracting business-focused insights from data. For most organizations, data science is employed to transform data into value in the form improved revenue, reduced costs, business agility, improved customer experience, the development of new products, and the like.
  
### State and justify the database schema and ETL pipeline:
 - **Database Schema:**
     - Fact Table
        | Table | Description |
        | ------ | ------ |
        | songplays | records in log data associated with song plays with only page = NextSong. This table will provides the metric of the business process |
     - Dimension Tables: The following tables can answer all questions related to **Where**, **When**, **What**
        | Table | Description |
        | ------ | ------ |
        | users | users in the app |
        | songs | songs in music database |
        | artists | artists in music database |
        | time | timestamps of records in **songplays** broken down into specific units |
 - **ETL Pipeline:**
  1. Run <i>etl.ipynb</i> in a Jupyter notebook to develop our ETL processes for each table. We will use a small subset of data as a sample data set for fast development. (The script here will be use later on the real ETL script file)
  2. Create **S3** <i>(s3a://salinee-bucket)</i> which will use for writing data to as parquet files.
  3. Run <i>etl.py</i> to simulate automate processes to read data as a JSON format from data source **S3**(s3a://udacity-dend), process them and write them back to destination **S3**(s3a://salinee-bucket) as a parquet files format.
