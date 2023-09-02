# How to Perform QA of an Entire Table or Dataset Using a Single Executable Athena Query (AWS)
### Introduction
Quality Assurance (QA) is an important part of any analyst's job. Whether you're just trying to get a high-level sense of what a dataset contains, looking for potential problems in preparing a dataset to be dashboard-ready, or aiming to explore the finer granularity of a dataset's various cross-sections and subsets, you're probably going to need to know your way around some query language to get there. 

The purpose of this repository is to show how one might go about using Amazon Web Service's (AWS) interactive query service (a.k.a. Athena) to perform QA or User Acceptance Testing (UAT) of an entire table or dataset using a single executable query. The intention is also to show and explain a few other tips and tricks that may come in handy along the way, as a means of hopefully helping you get up to speed on how to make AWS, S3, and Athena work for you. 

### The Audience
This repo is primarily intended for SQL users who are perhaps transitioning to AWS Athena and maybe haven't learned all the ropes yet.

### Acknowledgements
It's worth saying at the outset that there are obviously better tools for performing quick and thorough analysis of a dataset (this coder happens to find regular 'ole SQL to be way more versatile than what AWS Athena provides and allows, and would 9 times out of 10 rather be working in Python's Pandas library if it were up to him...) but it's also worth reminding folks that there are trade-offs to working in AWS. One of the benefits is that you get to be serverless, if that's something you're trying to get or be. I fully acknowledge that it doesn't make a whole lot of sense to use Athena to perform UAT or QA work on datasets of the sort you're likely to be storing in AWS's S3 service, but I would argue that there may be situations in one's life or work in which it might present itself as the quickest or the most efficient means of getting the job done.  

### The Process
While this tutorial promises to leave you with a query that will QA an entire dataset, the assumption is that you'll already be familiar with Athena and have your data stored there. Becuase of this, there are a few additional setup steps involved. From start to finish the procedure should look something as follows:

- **Step 1** : Upload Your Raw Data into AWS S3
- **Step 2** : Create a Table for Your Raw Data in AWS Athena
- **Step 3** : Write an Athean-compatible QA Query That Returns a Table in Which Each Row Corresponds To and Provides the Following Information About Each Column of Your Raw Dataset:
    - The name of the table in the Athena database
    - The name of the column being QA'd
    - The ordinal position in the table of the column being QA'd
    - The datatype of the data in the column being QA'd
    - A count of the null values in the column
    - A count of the non-null values in the column
    - A count of the distinct values in the column
    - A flag to indicate whether the column data contains duplicate entries
    - A flag to indicate whether the column data contains non-alphanumeric characters
    - A flag to indicate whether the column data contains letters
    - A count of the minimum number of characters contained in the column's non-null entries  
    - A count of the maximum number of characters contained in the columns' non-null entries
    - The maximum value of the column data, interpreted as a string
    - The minimum value of the column data, interpreted as a string
    - A flag to indicate whether the column contains numbers
    - A flag to indicate whether the column contains ONLY numbers
    - The maximum value of the column data, interpreted as an integer, if applicable
    - The minimum value of the column data, interpreted as an integer, if applicable
    - A flag to indicate whether the column contains decimals
    - A flag to indicate whether the column could be cast as a decimal datatype
    - The maximum value of the column data, interpreted as a decimal, if applicable
    - The minimum value of the column data, interpreted as a decimal, if applicable
    - A flag to indicate whether the column could be interpreted as a date
    - An indicator to describe the format of the date, if applicable
    - The maximum value of the column data, interpreted as a date datatype, if applicable
    - The minumum value of the column data, interpreted as a date datatype, if applicable

### Sample Data
For the purposes of this How-To, let's utilize a sample dataset from [Kaggle.com](https://www.kaggle.com/datasets/nelgiriyewithana/top-spotify-songs-2023?resource=download). This dataset contains data pertaining to the 'Most Streamed Spotify Songs in 2023'. A quick glance will show this data contains 28 columns and 953 rows.

![spotify data png](/Assets/images/Kaggle_data_screenshot_1.png)

# Step 1: Upload Your Raw Data into AWS S3
Hopefully if you're using AWS Athena, you're familiar with AWS's Simple Storage Service, or S3, as it's known. (If you're unfamiliar with S3, it's essentially Amazon's object storage service that offers industry-leading scalability, data availability, security, and performance. To read more about S3, check out the [AWS S3 documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html).)

To query data in Amazon Athena, you'll first want to upload your raw data files to S3. To do this:

1. Create an S3 bucket to store your raw data files. Your bucketname has to be unique, so give it a name that's not likely to already exist somewhere (e.g. "MyUniqueBucket_01").
2. Create a folder within your S3 bucket to hold your raw data file (e.g. "TopSpotifySongsFolder"). 
3. Upload the csv or whatever dataset you happen to be using to the folder within your S3 bucket.

> HEADS UP: When you call data into Athena using a 'CREATE TABLE' query, you will reference your data not according to the name of the file ('top-spotify-songs-2023.csv'), but rather according to the *location of the folder that houses the data* ('s3://MyUniqueBucket_01/TopSpotifySongsFolder'). Because of this, it's important to avoid storing multiple data files with differing datatypes, column amounts or column headers within the same folder in S3. Anything you intend to make a table from and query in Athena should be stored within its own folder.

Once you've created your S3 bucket and folder, your uploaded data should look something like this:

![data in S3](/Assets/images/Kaggle_data_screenshot_1.png)


# Step 2: Create a Table for Your Data in AWS Athena
Once your data is uploaded to S3, you can pull it into Athena with a CREATE TABLE script. 

Note that several of the column headers in our csv contain special characters (like '()' and '%'). Athena isn't going to like these. As a general rule, it's best not to include special characters in your column headers when working in AWS Athena. Underscores are the only 'special characters' allowed. For more on the specifications for table and column names in AWS Athena, check out the [AWS Athena special characters documentation](https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html).

When we create a table in Athena from S3, we're calling in the data, but we have the opportunity to rename the columns to whatever we want. Athena's schema maps to column location and uses delimiter specifications to distinguish where one column ends and another begins. In most cases, if we'd like to be consistent with the data file, we can use the same names for the columns that have been provided. Because we have special characters, I'm altering the following column names in the CREATE script:

| **ORIGINAL COLUMN NAME** | **NEW COLUMN NAME**        |
|--------------------------|----------------------------|
| artist(s)_name           | artist_name                |
| danceability_%           | danceability_percent       |
| valence_%                | valence_percent            |
| energy_% 	               | energy_percent             |
| acousticness_% 	       | acousticness_percent       |  
| instrumentalness_% 	   | instrumentalness_percent   |  
| liveness_% 	           | liveness_percent           |  
| speechiness_%            | speechiness_percent        |  

Open your query editor in Athena and create your table using the following format:

```
CREATE TABLE IF NOT EXISTS top_spotify_songs_2023_table (
    track_name string comment 'NULL'
    , artists_name string comment 'NULL'	
    , artist_count string comment 'NULL'	
    , released_year string comment 'NULL'	
    , released_month string comment 'NULL'	
    , released_day string comment 'NULL'	
    , in_spotify_playlists string comment 'NULL'	
    , in_spotify_charts string comment 'NULL'	
    , streams string comment 'NULL'	
    , in_apple_playlists string comment 'NULL'	
    , in_apple_charts string comment 'NULL'	
    , in_deezer_playlists string comment 'NULL'	
    , in_deezer_charts string comment 'NULL'	
    , in_shazam_charts string comment 'NULL'	
    , bpm string comment 'NULL'
    , key string comment 'NULL'	
    , mode string comment 'NULL'	
    , danceability_percent string comment 'NULL'	
    , valence_percent string comment 'NULL'	
    , energy_percent string comment 'NULL'	
    , acousticness_percent string comment 'NULL'	
    , instrumentalness_percent string comment 'NULL'	
    , liveness_percent string comment 'NULL'	
    , speechiness_percent string comment 'NULL'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORE AS TEXTFILE
LOCATION 's3://MyUniqueBucket_01/TopSpotifySongsFolder/'
TBLPROPERTIES ('classification' = 'csv',
    'skip.header.line.count' = '1');
```
> NOTE: Because we have a small dataset here, it's easy enough to open our csv file and take a peek at the column headers to discover things about the data that we need to know in order to know the best way to call our data in, like (1) how many columns there are and (2) the provided column names. If you're looking at a dataset for the first time and don't want to go to the trouble of downloading the csv, you can also view column names by querying the data using **S3 Select**. This is a handy feature that allows you to preview data using preloaded queries to view portions of your data. For more on how to use S3 Select, as well as limitations and requirements, check out the [S3 select documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/selecting-content-from-objects.html).

> HEADS UP: There may be instances when you're working in S3 and Athena when your data file is too large to open using S3 Select and may even be too large to safely download and open in any text editor. What to do? How are you supposed to retrieve column names to create a table in Athena if you can't see the data you're working with?

### How To Retrieve Column Names From a Dataset Stored in S3 That's Too Large to Download or Open, Using AWS Athena
A somewhat tedious (but useful, nonetheless) workaround to this problem involves creating a table using the format described above, but using placeholder names for the column headers and just taking a blind guess at how many columns you're likely to have. For instance, if the csv we're using in this example contained 17 million rows of data, it would probably be too large for us to open and view in excel or any text editor. But&#151we could create a table in which we retain the column headers provided by the data file. We do this by omitting from the ```TBLPROPERTIES``` definition the parameter ```'skip.header.line.count' = '1'```. We could then run the following script:

```
CREATE TABLE IF NOT EXISTS top_spotify_songs_2023_table (
    column1 string comment 'NULL'
    , column2 string comment 'NULL'	
    , column3 string comment 'NULL'	
    , column4 string comment 'NULL'	
    , column5 string comment 'NULL'	
    , column6 string comment 'NULL'	
    , column7 string comment 'NULL'	
    , column8 string comment 'NULL'	
    , column9 string comment 'NULL'	
    , column10 string comment 'NULL'	
    , column11 string comment 'NULL'
    , column12 string comment 'NULL'	
    , column13 string comment 'NULL'	
    , column14 string comment 'NULL'	
    , column15 string comment 'NULL'	
    , column16 string comment 'NULL'	
    , column17 string comment 'NULL'	
    , column18 string comment 'NULL'	
    , column19 string comment 'NULL'	
    , column20 string comment 'NULL'	
    , column21 string comment 'NULL'
    , column22 string comment 'NULL'	
    , column23 string comment 'NULL'	
    , column24 string comment 'NULL'	
    , column25 string comment 'NULL'	
    , column26 string comment 'NULL'	
    , column27 string comment 'NULL'	
    , column28 string comment 'NULL'	
    , column29 string comment 'NULL'	
    , column30 string comment 'NULL'	
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORE AS TEXTFILE
LOCATION 's3://MyUniqueBucket_01/TopSpotifySongsFolder/'
TBLPROPERTIES ('classification' = 'csv');
```
Note that the above script just uses 'columnX' for the name, and contains more columns than our known dataset. That's okay! In fact, if it's your first time creating a table from a dataset you've never seen and you don't know how many columns there are, it's better to try creating and recreating the table until you can confirm that you've reached the 'end' of the dataset&#151that is, until your data starts showing columns that are entirely null. 

Once you've created the table, you can view the first ten rows of data by running the following:

```select * from top_spotify_songs_2023_table limit 10```

![data in S3](/Assets/images/Kaggle_data_screenshot_1.png)

My recommendation from here would be to identify a column that appears to contain mostly *numeric* data(In our example dataset, 'column3' certainly looks promising). Because you've called in all the columns as strings (synonymous with varchars in Athena) you can then order the query by that column <ins>in reverse order</ins>. Because Athena processes numbers before letters when given string datat, the following query should reveal the column headers contained within the dataset (if there are any). 

```select column3 from top_spotify_songs_2023_table limit 10```

![data in S3](/Assets/images/Kaggle_data_screenshot_1.png)

Once you can view the column headers and confirm that you've arrived at the end of your data (because you are seeing null or unnamed columns), you can modify your ```CREATE TABLE IF NOT EXISTS``` script using the appropriate column names. Once your column names are updated, you can also add the ```'skip.header.line.count' = '1'``` parameter back to the ```TBLPROPERTIES``` definition. Drop the 'top_spotify_songs_2023_table' by running the following:

```DROP TABLE IF EXISTS top_spotify_songs_2023_table```

Finally, recreate 'top_spotify_songs_2023_table' using the updated script.

![data in S3](/Assets/images/Kaggle_data_screenshot_1.png)

# Step 3: Write an Athean-compatible QA Query That Returns a Table in Which Each Row Corresponds To and Provides Helpful Information About Each Column of Your Raw Dataset
Now for the fun part.