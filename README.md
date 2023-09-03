# How to Perform QA of an Entire Table or Dataset Using a Single Athena Query (AWS)
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
- **Step 2** : Create a Table for Your Raw Data in AWS Athena, Using Strings for the Datatypes
- **Step 3** : Rename Your Table and Columns to Make Your Query Universally Applicable
- **Step 4** : Write Individual Queries That Will:
    + **4a**. Get the Table's Metadata (table_name, column_name, ordinal_position, data_type)
    + **4b**. Get the Count of Null Values in a Column
    + **4c**. Get the Count of Non-Null and Distinct Values in a Column
    + **4d**. Create a Flag to Indicate Whether the Column Data Contains Duplicates
    + **4e**. Create a Flag to Indicate Whether the Column Data Contains Non-Alphanumeric Characters
    + **4f**. Create a Flag to Indicate Whether the Column Data Contains Letters
    + **4g**. Get the Minimum and Maximum Value of the Number of Characters Contained in the Column's Non-Null Entries
    + **4h**. Get the Alphabetical Minimum and Maximum Value of the Data in the Column's Non-Null Entries
    + **4i**. Create a Flag to Indicate Whether the Column Data Contains Numbers
    + **4j**. Create a Flag to Indicate Whether the Column Data Contains ONLY Numbers
    + **4k**. Get the Numeric Minimum and Maximum Value of the Data in the Column's Non-Null Entries, If Applicable
    + **4l**. Create a Flag to Indicate Whether the Column Data Contains Decimals
    + **4m**. Create a Flag to Indicate Whether the Column Data Could Be Cast as a Decimal DataType
    + **4n**. Get the Minimum and Maximum Value of the Data, Cast as Decimals, in the Column's Non-Null Entries, If Applicable
    + **4o**. Create an Indicator to Describe the Format of the Date, If Applicable
    + **4p**. Create a Flag to Indicate Whether the Column Data Could Be Interpreted as a Date
    + **4q**. Get the Minimum and Maximum Value of the Data, Interpreted as a Date DataType, If Applicable

- **Step 5** : Tie all the Queries From Step 4 Together Into a Single Continuous Query ('col1') That Will Perform QA on A Single Column of Your Table's Data
- **Step 6** : Duplicate The Col1 Query from Step 5 For the First 10 Columns of Your Table
- **Step 7** : Rewrite the Query From Step 5 To Handle All-Integer Datatypes
- **Step 8** : Rewrite the Query From Step 5 To Handle All-Decimal Datatypes
- **Step 9** : Rewrite the Query From Step 5 To Handle All-Date and Timestamp Datatypes
- **Step 10** : Arrange and Modify the Query to Suit Your Dataset

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

> HEADS UP: There may be instances when you're working in S3 and Athena when your data file is too large to open or view using S3 Select and may even be too large to safely download and open in any text editor. What to do? How are you supposed to retrieve column names to create a table in Athena if you can't see the data you're working with? For a solution to this problem, check out this Github repository on [S3 Column Name Retrieval](https://github.com/SamSteffen/S3_Column_Name_Retrieval).

# Step 3 : Rename Your Table and Columns to Make Your Query Universally Applicable
I know we just went through a whole rigamarole about naming our columns appropriately, but I want to take a minute to highlight the importance of writing a QA script that can be used not just on our example data, but on ANY dataset we might encounter. If we can utilize placeholder column names&mdash;and better yet, placeholder *table* names&mdash; in our QA script, we might be able to apply the same code to alot of different datasets by making very minor adjustments instead of having to update all our column names and table names to match whatever dataset we happen to be working with. 

Let's make our code universally applicable by retitling our 'top_spotify_songs_2023_table' table to 'table_name'. At the same time, let's retitle all our columns to column number alphanumerics like 'col1', 'col2', 'col3', etc. Athena will allow us to do both of these things using a simple ```WITH``` statement that specifies our new table name as well as existing or new column names:

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
```
The only thing to keep in mind here is that the number of columns you're specifying in the parentheses after your new table name must align with the number of columns that are actually being returned in your ```SELECT``` statement. In this case, we have 28 columns and we don't want to drop any of them, so we're renaming all 28.

The other thing to keep in mind about Athena's ```WITH``` statement is that it will only function as a precursor to another ```SELECT``` statement that is external to the ```WITH```statement itself. Hence running the code above will not actually do anything until you add another ```SELECT``` statement, like this:

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
        )
SELECT * from table_data;
```
![data in S3](/Assets/images/Kaggle_data_screenshot_1.png)

Also note that the external ```SELECT``` statement generated the same data as the internal ```SELECT * FROM top_spotify_songs_2023_table```, but specifies a new table called 'table_data' that contains new column names. 

# Step 4: Write and Save Individual Queries to Discover Useful Information About Your String Data
Now for the fun part. Let's construct a QA query that will do all your QA work for you!

The best way to go about writing a complex query is to start simple. Before we try to do everything at once, let's see if we can retrieve all the pieces we'll need to construct a QA query of the kind we're looking for, one step at a time. Let's start with the metadata.

### 4a. Get the Table's Metadata (table_name, column_name, ordinal_position, data_type)
Metadata is, quite literally, data about data. In this case, we're talking about retrieving data from the Athena table that we created in our ```CREATE TABLE IF NOT EXISTS``` script, not by looking directly at the data itself, but by querying the schema that's automatically stored when you create a table in Athena. You can think of the schema as the blueprint of how data from one table relates to data that may or may not be found in data within another.

If at this point you're wondering why you would ever want to know about the data's data, you're asking the right question. Metadata is super helpful for doing QA work. For the purpose of automating it, you can think of this as a way for our code to automatically tell us things like the names of tables that are stored in our database, the names of columns that are stored in our tables, how many columns a table contains, even what datatypes a particular column may contain. By exploring the metadata of an Athena table, we can understand the contents of the data much faster than we could by opening it and trying to look at all of it.

To retrieve metadata for a table that has been created using AWS Athena you can query the table's information_schema, specifying the ```table_name``` and ```table_schema``` in the query's ```WHERE``` statement, thus:
```
SELECT * 
FROM information_schema
WHERE table_schema = 'default'
AND table_name='top_spotify_songs_2023_table'
```
![data in S3](/Assets/images/Kaggle_data_screenshot_1.png)

To retrieve information exclusively about the *columns* of a table, try:

```
SELECT * 
FROM information_schema.columns
WHERE table_schema = 'default'
AND table_name='top_spotify_songs_2023_table'
```

![data in S3](/Assets/images/Kaggle_data_screenshot_1.png)

For the purposes of our QA procedure, we can specify the metadata we'll be interested in&mdash;namely, table_name, column_name, ordinal_position, and data_type:

```
SELECT distinct
table_name
, column_name
, ordinal_position
, data_type
FROM information_schema.columns
WHERE table_schema = 'default'
and table_name = 'sample_table'
order by ordinal_position
```

### 4b. Get the Count of Null Values in a Column

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
SELECT 
count(col1) null_count 
FROM table_data 
WHERE col1 is null 
or col1 like '' 
or col1 like ' '
```

### 4c. Get the Count of Non-Null and Distinct Values in a Column

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
SELECT 
count(col1) total_nonnull_count
, count(distinct col1) distinct_count
FROM table_data
WHERE col1 is not null
and col1 <> ''
and col1 <> ' '
```
### 4d. Create a Flag to Indicate Whether the Column Data Contains Duplicates

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
SELECT
a.total_nonnull_count
, a.distinct_count
, CASE WHEN a.total_nonnull_count > a.distinct_count THEN 'N' ELSE 'Y' END contains_duplicates
FROM (
    SELECT count(col1) total_nonnull_count
    , count(distinct col1) distinct_count 
    FROM table_data
    ) a
WHERE col1 is not null
and col1 <> '' 
and col1 <> ' '
```

### 4e. Create a Flag to Indicate Whether the Column Data Contains Non-Alphanumeric Characters
Let's try to define some of what we mean when we say 'non-alphanumeric characters.' This could include exclamation points(!), 'at' symbols (@), pound signs (#), dollar signs($), percent signs (%), carrots(^), ampersands (&), asterisks(*), mathematical and logical operators (- / | + < >), as well as other punctuation marks like single and double quotation marks, backticks, apostrophes, commas, dashes, hypens,underscores, periods/decimals and question marks.

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
SELECT
CASE WHEN non_alphanumeric_count > 0 THEN 'Y' ELSE 'N' END contains_non_alphanumerics
FROM (
    SELECT count(col1) non_alphanumeric_count
    FROM table_data
    WHERE regexp_like(col1, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\'|\"|\_|\|\')=True
)
```

### 4f. Create a Flag to Indicate Whether the Column Data Contains Letters
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
SELECT 
CASE WHEN contains_letters_count > 0 THEN 'Y' ELSE 'N' END contains_letters
FROM (
    SELECT count(col1) contains_letters_count
    FROM table_data
    WHERE regexp_like(col1, '[A-Za-z]')=True
)
```

### 4g. Get the Minimum and Maximum Value of the Number of Characters Contained in the Column's Non-Null Entries
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
SELECT
min(char_length) min_charlength
, max(char_length) max_charlength
FROM (
    SELECT
    distinct col1 data
    , length(col1) char_length
    FROM table_data
    WHERE col1 is not null 
    and col1 <> '' and col1 <> ' '
    )
```
### 4h. Get the Alphabetical Minimum and Maximum Value of the Data in the Column's Non-Null Entries

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
SELECT 
min(col1)
, max(col1)
FROM table_data
WHERE col1 is not null and col1 <> '' and col1 <> ' '
```
### 4i. Create a Flag to Indicate Whether the Column Data Contains Numbers
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
SELECT 
CASE WHEN contains_numbers_count > 0 THEN 'Y' ELSE 'N' END contains_numbers
FROM (
    SELECT count(col1) contains_numbers_count
    FROM table_data
    WHERE regexp_like(col1, '\d')=True
)
```
### 4j. Create a Flag to Indicate Whether the Column Data Contains ONLY Numbers
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
SELECT
CASE WHEN a.contains_numbers = 'Y'
    AND b.contains_letters = 'N'
    AND c.contains_non_alphanumerics = 'N' 
    THEN 'Y' ELSE 'N' END contains_only_numbers
FROM (
        SELECT 
        CASE WHEN contains_numbers_count > 0 THEN 'Y' ELSE 'N' END contains_numbers
        FROM (
            SELECT count(col1) contains_numbers_count
            FROM table_data
            WHERE regexp_like(col1, '\d')=True
        )        
    ) a
FULL OUTER JOIN (
        SELECT 
        CASE WHEN contains_letters_count > 0 THEN 'Y' ELSE 'N' END contains_letters
        FROM (
            SELECT count(col1) contains_letters_count
            FROM table_data
            WHERE regexp_like(col1, '[A-Za-z]')=True
        )
    ) b
    ON 1=1
FULL OUTER JOIN (
    SELECT
    CASE WHEN non_alphanumeric_count > 0 THEN 'Y' ELSE 'N' END contains_non_alphanumerics
        FROM (
            SELECT count(col1) non_alphanumeric_count
            FROM table_data
            WHERE regexp_like(col1, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\'|\"|\_|\|\')=True
        )
    ) c
    ON 1=1
```
### 4k. Get the Numeric Minimum and Maximum Value of the Data in the Column's Non-Null Entries, If Applicable
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
SELECT
CASE WHEN B.contains_only_numbers = 'Y' THEN cast(A.numeric_max as varchar) ELSE 'N/A' END numeric_max
, CASE WHEN B.contains_only_numbers = 'Y' THEN cast(A.numeric_min as varchar) ELSE 'N/A' END numeric_min
FROM (
    SELECT
    max(try(cast(col1 as int))) numeric_max
    , min(try(cast(col1 as int))) numeric_min
    FROM table_data 
    WHERE col1 is not null and col1 <> '' and col1 <> ' '
) A
FULL OUTER JOIN (
    SELECT
    CASE WHEN a.contains_numbers = 'Y'
        AND b.contains_letters = 'N'
        AND c.contains_non_alphanumerics = 'N' 
        THEN 'Y' ELSE 'N' END contains_only_numbers
    FROM (
            SELECT 
            CASE WHEN contains_numbers_count > 0 THEN 'Y' ELSE 'N' END contains_numbers
            FROM (
                SELECT count(col1) contains_numbers_count
                FROM table_data
                WHERE regexp_like(col1, '\d')=True
            )        
        ) a
    FULL OUTER JOIN (
            SELECT 
            CASE WHEN contains_letters_count > 0 THEN 'Y' ELSE 'N' END contains_letters
            FROM (
                SELECT count(col1) contains_letters_count
                FROM table_data
                WHERE regexp_like(col1, '[A-Za-z]')=True
            )
        ) b
        ON 1=1
    FULL OUTER JOIN (
        SELECT
        CASE WHEN non_alphanumeric_count > 0 THEN 'Y' ELSE 'N' END contains_non_alphanumerics
            FROM (
                SELECT count(col1) non_alphanumeric_count
                FROM table_data
                WHERE regexp_like(col1, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\'|\"|\_|\|\')=True
            )
        ) c
        ON 1=1
) B
ON 1=1
```
### 4l. Create a Flag to Indicate Whether the Column Data Contains Decimals
While this information would already be caught by the flag we created to locate non-alphanumeric characters, isolating the decimal from other non-alphanumerics can be useful for determining whether the data found in this column could potentially be classified as a decimal datatype.
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
SELECT
CASE WHEN decimal_count > 0 THEN 'Y' ELSE 'N' END contains_decimals
FROM (
    SELECT 
    count(col1) decimal_count
    FROM table_data
    WHERE regexp_like(col1, '\.')=True
) 
```
### 4m. Create a Flag to Indicate Whether the Column Data Could Be Cast as a Decimal DataType
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
SELECT 
CASE WHEN b.decimal_count = a.distinct_count THEN 'Y' ELSE 'N' END decimal_datatype_flag 
FROM (
    SELECT  
    count(distinct col1) distinct_count
    FROM table_data
    WHERE col1 is not null and col1 <> '' and col1 <> ' '
) a
FULL OUTER JOIN (
        SELECT 
        count(col1) decimal_count
        FROM table_data
        WHERE regexp_like(col1, '\.')=True
) b
ON 1=1
```
### 4n. Get the Minimum and Maximum Value of the Data, Cast as Decimals, in the Column's Non-Null Entries, If Applicable
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
SELECT 
CASE WHEN decimal_max is not null then cast(decimal_max as varchar) else 'N/A' end decimal_max
, CASE WHEN decimal_min is not null then cast(decimal_min as varchar) else 'N/A' end decimal_min
FROM (
    SELECT 
    max(try(cast(col1 as decimal(18,2)))) decimal_max
    , min(try(cast(col1 as decimal(18,2)))) decimal_min
    FROM table_data
    WHERE col1 is not null and col1 <> '' and col1 <> ' '
)
```
### 4o. Create an Indicator to Describe the Format of the Date, If Applicable
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
SELECT 
b.date_format as date_format_indicator
FROM (
    SELECT distinct
    a.date_format
    , count(a.date_format) date_count
    FROM (
        SELECT
        col1
        , CASE 
        WHEN regexp_like(col1, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
            THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
        WHEN regexp_like(col1, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
            THEN 'yyyy-mm-dd hh:mm:ss.mmm'
        WHEN regexp_like(col1, '\d{4}\-\d{2}\-\d{2}$')
            THEN 'yyyy-mm-dd'
        WHEN regexp_like(col1, '\d{4}\-\d{1,2}\-\d{1,2}$')
            THEN 'yyyy-m-d'
        WHEN regexp_like(col1, '\d{2}\-\d{2}\-\d{4}')
            THEN 'mm-dd-yyyy'
        WHEN regexp_like(col1, '\d{1,2}\-\d{1,2}\-\d{4}')
            THEN 'm-d-yyyy'
        WHEN regexp_like(col1, '\d{4}\/\d{2}\/\d{2}$')
            THEN 'yyyy/mm/dd'
        WHEN regexp_like(col1, '\d{4}\/\d{1,2}\/\d{1,2}$')
            THEN 'yyyy/m/d'
        WHEN regexp_like(col1, '\d{2}\/\d{2}\/\d{4}')
            THEN 'mm/dd/yyyy'
        WHEN regexp_like(col1, '\d{1,2}\/\d{1,2}\/\d{4}$')
            THEN 'm/d/yyyy'
        WHEN regexp_like(col1, '^\d{8}$')
            THEN 'yyyymmdd'
        ELSE 'N/A' END as date_format
        FROM table_data
        WHERE col1 is not null and col1 <> '' and col1 <> ' '
    ) a
    GROUP BY a.date_format
) b
```

### 4p. Create a Flag to Indicate Whether the Column Data Could Be Interpreted as a Date

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
SELECT 
CASE WHEN e.date_flag <> 'N/A' THEN 'Y' ELSE 'N' END date_flag
FROM (
    SELECT
    CASE 
        WHEN d.date_format <> 'N/A' AND d.date_count = d.total_nonnull_count
        THEN d.date_format 
        ELSE 'N/A' END date_flag
    FROM (
        SELECT
        c.total_nonnull_count
        , b.date_count
        , b.date_format
        FROM (
            SELECT count(col1) total_nonnull_count 
            FROM table_data 
            WHERE col1 is not null and col1 <> '' and col1 <> ' ' 
        ) c
        FULL OUTER JOIN (
            SELECT distinct
            a.date_format
            , count(a.date_format) date_count
            FROM (
                SELECT
                col1
                , CASE 
                WHEN regexp_like(col1, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                    THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                WHEN regexp_like(col1, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                    THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                WHEN regexp_like(col1, '\d{4}\-\d{2}\-\d{2}$')
                    THEN 'yyyy-mm-dd'
                WHEN regexp_like(col1, '\d{4}\-\d{1,2}\-\d{1,2}$')
                    THEN 'yyyy-m-d'
                WHEN regexp_like(col1, '\d{2}\-\d{2}\-\d{4}')
                    THEN 'mm-dd-yyyy'
                WHEN regexp_like(col1, '\d{1,2}\-\d{1,2}\-\d{4}')
                    THEN 'm-d-yyyy'
                WHEN regexp_like(col1, '\d{4}\/\d{2}\/\d{2}$')
                    THEN 'yyyy/mm/dd'
                WHEN regexp_like(col1, '\d{4}\/\d{1,2}\/\d{1,2}$')
                    THEN 'yyyy/m/d'
                WHEN regexp_like(col1, '\d{2}\/\d{2}\/\d{4}')
                    THEN 'mm/dd/yyyy'
                WHEN regexp_like(col1, '\d{1,2}\/\d{1,2}\/\d{4}$')
                    THEN 'm/d/yyyy'
                WHEN regexp_like(col1, '^\d{8}$')
                    THEN 'yyyymmdd'
                ELSE 'N/A' END as date_format
                FROM table_data
                WHERE col1 is not null and col1 <> '' and col1 <> ' ' 
            ) a
            GROUP BY date_format
        ) b
        ON 1=1
    ) d
) e
```
### 4q. Get the Minimum and Maximum Value of the Data, Interpreted as a Date DataType, If Applicable
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
SELECT
CASE 
    WHEN i.date_flag = 'mm/dd/yyyy' OR i.date_flag = 'm/d/yyyy' THEN i.max_date1
    WHEN i.date_flag = 'yyyy/mm/dd' OR i.date_flag = 'yyyy/m/d' THEN i.max_date2
    WHEN i.date_flag = 'mm-dd-yyyy' OR i.date_flag = 'm-d-yyyy' THEN i.max_date3
    WHEN i.date_flag = 'yyyy-mm-dd' OR i.date_flag = 'yyyy-m-d' THEN i.max_date4
    WHEN i.date_flag = 'yyyymmdd' THEN i.max_date5
    WHEN i.date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR i.date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN i.max_date6
    ELSE 'N/A' END AS max_date
, CASE
    WHEN i.date_flag = 'mm/dd/yyyy' OR i.date_flag = 'm/d/yyyy' THEN i.min_date1
    WHEN i.date_flag = 'yyyy/mm/dd' OR i.date_flag = 'yyyy/m/d' THEN i.min_date2
    WHEN i.date_flag = 'mm-dd-yyyy' OR i.date_flag = 'm-d-yyyy' THEN i.min_date3
    WHEN i.date_flag = 'yyyy-mm-dd' OR i.date_flag = 'yyyy-m-d' THEN i.min_date4
    WHEN i.date_flag = 'yyyymmdd' THEN i.min_date5
    WHEN i.date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR i.date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN i.min_date6
    ELSE 'N/A' END AS min_date
FROM (
    SELECT 
    CASE WHEN a.max_date1 is not null THEN cast(a.max_date1 as varchar) else 'N/A' END max_date1
    , CASE WHEN a.min_date1 is not null THEN cast(a.min_date1 as varchar) else 'N/A' END min_date1
    , CASE WHEN b.max_date2 is not null THEN cast(b.max_date2 as varchar) else 'N/A' END max_date2
    , CASE WHEN b.min_date2 is not null THEN cast(b.min_date2 as varchar) else 'N/A' END min_date2
    , CASE WHEN c.max_date3 is not null THEN cast(c.max_date3 as varchar) else 'N/A' END max_date3
    , CASE WHEN c.min_date3 is not null THEN cast(c.min_date3 as varchar) else 'N/A' END min_date3
    , CASE WHEN d.max_date4 is not null THEN cast(d.max_date4 as varchar) else 'N/A' END max_date4
    , CASE WHEN d.min_date4 is not null THEN cast(d.min_date4 as varchar) else 'N/A' END min_date4
    , CASE WHEN f.max_date5 is not null THEN cast(f.max_date5 as varchar) else 'N/A' END max_date5
    , CASE WHEN f.min_date5 is not null THEN cast(f.min_date5 as varchar) else 'N/A' END min_date5
    , CASE WHEN g.max_date6 is not null THEN cast(g.max_date6 as varchar) else 'N/A' END max_date6
    , CASE WHEN g.min_date6 is not null THEN cast(g.min_date6 as varchar) else 'N/A' END min_date6
    , h.date_flag
    FULL OUTER JOIN  (
        SELECT
        max(
            try(
                cast(
                    concat(
                        --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                        split_part(col1,'/',3),'-',split_part(col1,'/',1),'-',split_part(col1,'/',2)
                        ) 
                    as date)
                )
            ) max_date1
        , min(
            try(
                cast(
                    concat(
                        split_part(col1,'/',3),'-',split_part(col1,'/',1),'-',split_part(col1,'/',2)
                        )
                    as date)
                )
            ) min_date1
        FROM table_data
        WHERE col1 is not null and col1 <> '' and col1 <> ' '
    ) a
    ON 1=1
    FULL OUTER JOIN (
        SELECT
        max(
            try(
                cast(
                    concat(
                        --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                        split_part(col1,'/',1),'-',split_part(col1,'/',2),'-',split_part(col1,'/',3)
                        ) 
                    as date)
                )
            ) max_date2
        , min(
            try(
                cast(
                    concat(
                        split_part(col1,'/',1),'-',split_part(col1,'/',2),'-',split_part(col1,'/',3)
                        )
                    as date)
                )
            ) min_date2
        FROM table_data
        WHERE col1 is not null and col1 <> '' and col1 <> ' '
    ) b
    ON 1=1
    FULL OUTER JOIN (
        SELECT
        max(
            try(
                cast(
                    concat(
                        --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                        split_part(col1,'-',3),'-',split_part(col1,'-',1),'-',split_part(col1,'-',2)
                        ) 
                    as date)
                )
            ) max_date3
        , min(
            try(
                cast(
                    concat(
                        split_part(col1,'-',3),'-',split_part(col1,'-',1),'-',split_part(col1,'-',2)
                        )
                    as date)
                )
            ) min_date3
        FROM table_data
        WHERE col1 is not null and col1 <> '' and col1 <> ' '
    ) c
    ON 1=1
    FULL OUTER JOIN (
        SELECT
        max(
            try(
                cast(
                    --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                    col1
                    as date)
                )
            ) max_date4
        , min(
            try(
                cast(
                    --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                    col1
                    as date)
                )
            ) min_date4
        FROM table_data
        WHERE col1 is not null and col1 <> '' and col1 <> ' '
    ) d
    ON 1=1
    FULL OUTER JOIN (
        SELECT 
        CASE WHEN substr(cast(e.max_int as varchar), 1, 2) in ('19','20') 
            THEN CAST(e.max_date as varchar) 
            ELSE 'N/A' END max_date5
        , CASE WHEN substr(cast(e.min_int as varchar), 1, 2) in ('19','20')
            THEN CAST(e.min_date as varchar)
            ELSE 'N/A' END min_date5
        FROM (
        SELECT 
        max(try(cast(col1 as int))) max_int
        , min(try(cast(col1 as int))) min_int
        , max(
            try(
                cast(
                    concat(
                        --when data is formatted as 'yyyymmdd' use:
                        substr(col1, 1, 4),'-',substr(col1, 5, 2),'-', substr(col1, 7, 2)
                        )
                    as date)
                )
            ) max_date
        , min(
            try(
                cast(
                    concat(
                        --when data is formatted as 'yyyymmdd' use:
                        substr(col1, 1, 4),'-',substr(col1, 5, 2),'-', substr(col1, 7, 2)
                        )
                    as date)
                )
            ) min_date
        FROM table_data
        WHERE col1 is not null and col1 <> '' and col1 <> ' '
        ) e
    ) f
    ON 1=1
    FULL OUTER JOIN (
        SELECT
        --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
        max(try(cast(col1 as date))) max_date6
        , min(try(cast(col1 as date))) min_date6
        FROM table_data
        WHERE col1 is not null and col1 <> '' and col1 <> ' '
    ) g
    ON 1=1
    FULL OUTER JOIN (
    SELECT 
    CASE WHEN E.date_flag <> 'N/A' THEN 'Y' ELSE 'N' END date_flag
    FROM (
        SELECT
        CASE 
            WHEN D.date_format <> 'N/A' AND D.date_count = D.total_nonnull_count
            THEN D.date_format 
            ELSE 'N/A' END date_flag
        FROM (
            SELECT
            C.total_nonnull_count
            , B.date_count
            , B.date_format
            FROM (
                SELECT count(col1) total_nonnull_count 
                FROM table_data 
                WHERE col1 is not null and col1 <> '' and col1 <> ' ' 
            ) C
            FULL OUTER JOIN (
                SELECT distinct
                A.date_format
                , count(A.date_format) date_count
                FROM (
                    SELECT
                    col1
                    , CASE 
                    WHEN regexp_like(col1, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col1, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col1, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col1, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col1, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col1, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col1, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col1, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col1, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col1, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col1, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                    WHERE col1 is not null and col1 <> '' and col1 <> ' ' 
                ) A
                GROUP BY date_format
            ) B
            ON 1=1
        ) D
    ) E
    ) h
) i
```
# Step 5 : Tie all the Queries From Step 4 Together Into a Single Continuous Query ('col1') That Will Perform All of the QA Procedures Outlined in Step 4 on A Single Column of Your Table's Data

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
, metadata as (
    SELECT distinct
    table_name
    , column_name
    , ordinal_position
    , data_type
    FROM information_schema.columns
    WHERE table_schema='default'
    AND table_name = 'top_spotify_songs_2023_table'
    ORDER BY ordinal_position
)
, metadata2 as (
    SELECT m.*
    , concat('col', cast(m.ordinal_position as varchar(3))) temp_column_name
    FROM metadata m
)
, col1_str (
    SELECT
    md.table_name, md.column_name, md.ordinal_position, me.data_type, md.temp_column_name
    , null_count, total_nonnull_count, distinct_count, contains_duplicates, contains_non_alphanumerics
    , contains_letters, min_charlength, max_charlength, alphabet_min, alphabet_max
    , contains_numbers, contains_only_numbers
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_min else 'N/A' END numeric_min
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_max else 'N/A' END numeric_max
    , contains_decimals, decimal_datatype_flag
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_min else 'n/A' END decimal_min
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_max else 'n/A' END decimal_max
    , CASE WHEN date_flag <> 'N/A' THEN 'Y' ELSE 'N' END date_flag
    , CASE WHEN date_flag <> 'N/A' THEN date_flag ELSE 'N/A' END date_form
    , CASE 
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN max_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN max_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN max_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN max_date4
        WHEN date_flag = 'yyyymmdd' THEN max_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN max_date6
        ELSE 'N/A' END AS max_date
    , CASE
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN min_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN min_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN min_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN min_date4
        WHEN date_flag = 'yyyymmdd' THEN min_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN min_date6
        ELSE 'N/A' END AS min_date
    FROM (
        SELECT 
        a.null_count, a.total_nonnull_count, a.distinct_count
        , CASE WHEN a.total_nonnull_count = a.distinct_count THEN 'N' ELSE 'Y' END contains_duplicates
        , CASE WHEN a.has_non_alphanumeric_characters_count = 0 THEN 'N' ELSE 'Y' END contains_non_alphanumerics

        , CASE WHEN a.has_letters_count = 0 THEN 'N' ELSE 'Y' END contains_letters
        , a.min_charlength, a.max_charlength, a.alphabet_min, a.alphabet_max

        , CASE WHEN a.has_numbers_count = 0 THEN 'N' ELSE 'Y' END contains_numbers
        , CASE WHEN a.has_numbers_count > 0 AND a.has_letters_count = 0 THEN 'Y' ELSE 'N' END contains_only_numbers
        , CASE WHEN b.numeric_max is not null then cast(b.numeric_max as varchar) ELSE 'N/A' END numeric_max
        , CASE WHEN b.numeric_min is not null then cast(b.numeric_min as varchar) ELSE 'N/A' END numeric_min

        , CASE WHEN a.has_decimals_count = 0 THEN 'N' ELSE 'Y' END contains_decimals
        , CASE WHEN a.decimal_datatype_count = a.distinct_count then 'Y' ELSE 'N' END decimal_datatype_flag
        , CASE WHEN c.decimal_max is not null THEN cast(c.decimal_max as varchar) ELSE 'N/A' END decimal_max
        , CASE WHEN c.decimal_min is not null THEN cast(c.decimal_min as varchar) ELSE 'N/A' END decimal_min

        , CASE WHEN a.dateformat <> 'N/A' AND a.date_count = a.total_nonnull_count THEN a.dateformat ELSE 'N/A' END date_flag
        , CASE WHEN d.max_date1 IS NOT NULL THEN CAST(d.max_date1 as varchar) ELSE 'N/A' END max_date1
        , CASE WHEN d.min_date1 IS NOT NULL THEN CAST(d.min_date1 as varchar) ELSE 'N/A' END min_date1
        , CASE WHEN e.max_date2 IS NOT NULL THEN CAST(e.max_date2 as varchar) ELSE 'N/A' END max_date2
        , CASE WHEN e.min_date2 IS NOT NULL THEN CAST(e.min_date2 as varchar) ELSE 'N/A' END min_date2
        , CASE WHEN f.max_date3 IS NOT NULL THEN CAST(f.max_date3 as varchar) ELSE 'N/A' END max_date3
        , CASE WHEN f.min_date3 IS NOT NULL THEN CAST(f.min_date3 as varchar) ELSE 'N/A' END min_date3
        , CASE WHEN g.max_date4 IS NOT NULL THEN CAST(g.max_date4 as varchar) ELSE 'N/A' END max_date4
        , CASE WHEN g.min_date4 IS NOT NULL THEN CAST(g.min_date4 as varchar) ELSE 'N/A' END min_date4
        , CASE WHEN h.max_date5 IS NOT NULL THEN CAST(h.max_date5 as varchar) ELSE 'N/A' END max_date5
        , CASE WHEN h.min_date5 IS NOT NULL THEN CAST(h.min_date5 as varchar) ELSE 'N/A' END min_date5
        , CASE WHEN i.max_date6 IS NOT NULL THEN CAST(i.max_date6 as varchar) ELSE 'N/A' END max_date6
        , CASE WHEN i.min_date6 IS NOT NULL THEN CAST(i.min_date6 as varchar) ELSE 'N/A' END min_date6
        FROM (
            SELECT
            a.null_count null_count
            , count(col1) total_nonnull_count
            , count(distinct col1) distinct_count
            , min(length(col1)) min_charlength
            , max(length(col1)) max_charlength
            , min(col1) alphabet_min
            , max(col1) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col1) null_count
                FROM table_data
                WHERE col1 is null or col1 LIKE '' or col1 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col1) has_numbers_count
                FROM table_data
                WHERE regexp_like(col1, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col1) has_letters_count
                FROM table_data
                WHERE regexp_like(col1, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col1) has_decimals_count
                FROM table_data
                WHERE regexp_like(col1, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col1) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col1, '\!|\@|\#|\$|\%|\^|\&|\*
                                        |\+|\-|\/|\\|\<|\>|\,|\.
                                        |\?|\||\'|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col1) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col1, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col1) date_count
                FROM table_data
                WHERE regexp_like(col1, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
                                        |\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$
                                        |\d{4}\-\d{2}\-\d{2}$
                                        |\d{4}\-\d{1,2}\-\d{1,2}$
                                        |\d{2}\-\d{2}\-\d{4}
                                        |\d{1,2}\-\d{1,2}\-\d{4}
                                        ||\d{4}\/\d{2}\/\d{2}$
                                        |\d{4}\/\d{1,2}\/\d{1,2}$
                                        |\d{2}\/\d{2}\/\d{4}
                                        |\d{1,2}\/\d{1,2}\/\d{4}$
                                        |^\d{8}$'
                                        )=True
            ) g
            ON 1=1
            FULL OUTER JOIN (
                SELECT distinct
                dateformat
                , count(dateformat) format_count
                FROM (
                    SELECT
                    col1
                    , CASE 
                    WHEN regexp_like(col1, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col1, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col1, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col1, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col1, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col1, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col1, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col1, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col1, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col1, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col1, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col1 is not null and col1 <> '' and col1 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col1 as int))) numeric_max
            , min(try(cast(col1 as int))) numeric_min
            FROM table_data
            WHERE col1 is not null and col1 <> '' and col1 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col1 as decimal(18,2)))) decimal_max
            , min(try(cast(col1 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col1 is not null and col1 <> '' and col1 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col1,'/',3),'-',split_part(col1,'/',1),'-',split_part(col1,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col1,'/',3),'-',split_part(col1,'/',1),'-',split_part(col1,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col1 is not null and col1 <> '' and col1 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col1,'/',1),'-',split_part(col1,'/',2),'-',split_part(col1,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col1,'/',1),'-',split_part(col1,'/',2),'-',split_part(col1,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col1 is not null and col1 <> '' and col1 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col1,'-',3),'-',split_part(col1,'-',1),'-',split_part(col1,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col1,'-',3),'-',split_part(col1,'-',1),'-',split_part(col1,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col1 is not null and col1 <> '' and col1 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col1
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col1
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col1 is not null and col1 <> '' and col1 <> ' '
        ) g
        ON 1=1
        FULL OUTER JOIN (
            SELECT 
            CASE WHEN substr(cast(A.max_int as varchar), 1, 2) in ('19','20') 
                THEN CAST(A.max_date as varchar) 
                ELSE 'N/A' END max_date5
            , CASE WHEN substr(cast(A.min_int as varchar), 1, 2) in ('19','20')
                THEN CAST(A.min_date as varchar)
                ELSE 'N/A' END min_date5
            FROM (
                SELECT 
                max(try(cast(col1 as int))) max_int
                , min(try(cast(col1 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col1, 1, 4),'-',substr(col1, 5, 2),'-', substr(col1, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col1, 1, 4),'-',substr(col1, 5, 2),'-', substr(col1, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col1 is not null and col1 <> '' and col1 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col1 as date))) max_date6
            , min(try(cast(col1 as date))) min_date6
            FROM table_data
            WHERE col1 is not null and col1 <> '' and col1 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col1'
)
, combined as (
SELECT * FROM col1_str
)
SELECT * FROM combined ORDER BY ordinal_position;
```
# Step 6 : Duplicate The Col1 Query from Step 5 For the First 10 Columns of Your Table
To duplicate the Col1 script to be applicable to additonal columns, I recommend copying and pasting the above query into a new query window in your Athena query editor. Once there, you can change the 'col1' variable to 'col2' everywhere it appears in the query using Athena's FIND & REPLACE function. To summon the FIND & REPLACE menu, click anywhere in the query editor window and hit 'Ctrl + F' to open the FIND & REPLACE window. Once open, you can select the '+' sign to open the 'REPLACE' window. Enter 'col1' in the FIND searchbar and 'col2' in the REPLACE WITH bar and select 'REPLACE ALL'.

Once the 'col1' variable has been replaced with 'col2', highlight the entire script and paste it back into the original query, directly below your col1 query from Step 5. 

Repeat the above process until your query resembles the following:

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28
    ) as (
        SELECT * FROM 'top_spotify_songs_2023_table
        )
, metadata as (
    SELECT distinct
    table_name
    , column_name
    , ordinal_position
    , data_type
    FROM information_schema.columns
    WHERE table_schema='default'
    AND table_name = 'top_spotify_songs_2023_table'
    ORDER BY ordinal_position
)
, metadata2 as (
    SELECT m.*
    , concat('col', cast(m.ordinal_position as varchar(3))) temp_column_name
    FROM metadata m
)
, col1_str (--)
, col2_str (--)
, col3_str (--)
, col4_str (--)
, col5_str (--)
, col6_str (--)
, col7_str (--)
, col8_str (--)
, col9_str (--)
, col10_str (--)
, combined as (
    SELECT * FROM col1_str
    UNION SELECT * FROM col2_str
    UNION SELECT * FROM col3_str
    UNION SELECT * FROM col4_str
    UNION SELECT * FROM col5_str
    UNION SELECT * FROM col6_str
    UNION SELECT * FROM col7_str
    UNION SELECT * FROM col8_str
    UNION SELECT * FROM col9_str
    UNION SELECT * FROM col10_str
)
Select * from combined ORDER BY ordinal_position;
```
# Step 7 : Rewrite the Query From Step 5 To Handle All-Integer Datatypes
```
insert code here
```
# Step 8 : Rewrite the Query From Step 5 To Handle All-Decimal Datatypes
```
insert code here
```
# Step 9 : Rewrite the Query From Step 5 To Handle All-Date and Timestamp Datatypes
```
insert code here
```
# Step 10 : Arrange and Modify the Query to Suit Your Dataset

# Limitations of This Query

# Conclusions