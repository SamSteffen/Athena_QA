# How to Perform QA on Up To 10 Columns of a Dataset Using a Single Athena Query (AWS)
### Introduction
Quality Assurance (QA) is an important part of any analyst's job. Whether you're just trying to get a high-level sense of what a dataset contains, looking for potential problems in preparing a dataset to be dashboard-ready, or aiming to explore the finer granularity of a dataset's various cross-sections and subsets, you're probably going to need to know your way around some query language to get there. 

The purpose of this repository is to show how one might go about using Amazon Web Service's (AWS) interactive query service (a.k.a. Athena) to perform QA or User Acceptance Testing (UAT) of ten columns of any given table or dataset using a single executable query. "Why ten?" you ask? "Why not *all*?" The reason our QA procedure cannot perform QA of a table containing more than 10 columns has to do with the length of the query we'll be crafting, that will push upon the boundaries of what Athena is capable of handling. But fear not--there's no reason we can't duplicate the query once it's written and use it, or slightly varied versions thereof, to query an entire dataset, containing as many columns as you care to throw at it.

The intention of creating this repository is also to show and explain a few other tips and tricks that may come in handy along the way, as a means of hopefully helping you get up to speed on how to make AWS, S3, and Athena work for you. 

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
- **Step 7** : Rewrite the Query From Step 5 To Handle Integer Datatypes
- **Step 8** : Rewrite the Query From Step 5 To Handle Decimal Datatypes
- **Step 9** : Arrange and Modify the Query to Suit Your Dataset

### Sample Data
For the purposes of this How-To, let's utilize a sample dataset from [Kaggle.com](https://www.kaggle.com/datasets/nelgiriyewithana/top-spotify-songs-2023?resource=download). This dataset contains data pertaining to the 'Most Streamed Spotify Songs in 2023'. A quick glance will show this data contains 24 columns and 953 rows.

![spotify data png](/Assets/images/Kaggle_data_screenshot_1.png)

# Step 1: Upload Your Raw Data into AWS S3
Hopefully if you're using AWS Athena, you're familiar with AWS's Simple Storage Service, or S3, as it's known. (If you're unfamiliar with S3, it's essentially Amazon's object storage service that offers industry-leading scalability, data availability, security, and performance. To read more about S3, check out the [AWS S3 documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html).)

To query data in Amazon Athena, you'll first want to upload your raw data files to S3. To do this:

1. Create an S3 bucket to store your raw data files. Your bucketname has to be unique, so give it a name that's not likely to already exist somewhere (e.g. "MyUniqueBucket_01").
2. Create a folder within your S3 bucket to hold your raw data file (e.g. "TopSpotifySongsFolder"). 
3. Upload the csv or whatever dataset you happen to be using to the folder within your S3 bucket.

> HEADS UP: When you call data into Athena using a ```CREATE TABLE``` query, you will reference your data not according to the name of the file ('top-spotify-songs-2023.csv'), but rather according to the *location of the folder that houses the data* ('s3://MyUniqueBucket_01/TopSpotifySongsFolder'). Because of this, it's important to avoid storing multiple data files with differing datatypes, column amounts or column headers within the same folder in S3. Anything you intend to make a table from and query in Athena should be stored within its own folder.

# Step 2: Create a Table for Your Data in AWS Athena
Once your data is uploaded to S3, you can pull it into Athena with a ```CREATE TABLE``` script. 

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
NOTE: Because we have a small dataset here, it's easy enough to open our csv file and take a peek at the column headers to discover things about the data that we need to know in order to know the best way to call our data in, like (1) how many columns there are and (2) the provided column names. 

If you're looking at a dataset for the first time and don't want to go to the trouble of downloading the csv, you can also view column names by querying the data using **S3 Select**. This is a handy feature that allows you to preview data using preloaded queries to view portions of your data. For more on how to use S3 Select, as well as limitations and requirements, check out the [S3 select documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/selecting-content-from-objects.html).

> HEADS UP: There may be instances when you're working in S3 and Athena when your data file is too large to open or view using S3 Select and may even be too large to safely download and open in any text editor. What to do? How are you supposed to retrieve column names to create a table in Athena if you can't see the data you're working with? For a solution to this problem, check out this Github repository on [S3 Column Name Retrieval](https://github.com/SamSteffen/S3_Column_Name_Retrieval).

# Step 3 : Rename Your Table and Columns to Make Your Query Universally Applicable
I know we just went through a whole rigamarole about naming our columns appropriately, but I want to take a minute to highlight the importance of writing a QA script that can be used not just on our example data, but on ANY dataset we might encounter. If we can utilize placeholder column names&mdash;and better yet, placeholder *table* names&mdash; in our QA script, we might be able to apply the same code to alot of different datasets by making very minor adjustments instead of having to update all our column names and table names to match whatever dataset we happen to be working with. 

Let's make our code universally applicable by retitling our top_spotify_songs_2023_table' table to **'table_name'**. At the same time, let's retitle all our columns to column number alphanumerics like **'col1', 'col2', 'col3'**, etc. Athena will allow us to do both of these things using a simple ```WITH``` statement that specifies our new table name as well as existing or new column names:

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
        )
```
The only thing to keep in mind here is that the number of columns you're specifying in the parentheses after your new table name must align with the number of columns that are actually being returned in your ```SELECT``` statement. In this case, we have 24 columns and we don't want to drop any of them, so we're renaming all 24.

The other thing to keep in mind about Athena's ```WITH``` statement is that it will only function as a precursor to another ```SELECT``` statement that is external to the ```WITH```statement itself. Hence running the code above will not actually do anything until you add another ```SELECT``` statement, like this:

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
        )
SELECT * from table_data;
```
Also note that the external ```SELECT``` statement generates the same data as the internal ```SELECT * FROM top_spotify_songs_2023_table```, but specifies a new table called 'table_data' that contains new column names. 

# Step 4: Write and Save Individual Queries to Discover Useful Information About Your String Data
Now for the fun part. Let's construct a QA query that will do all your QA work for you!

The best way to go about writing a complex query is to start simple. Before we try to do everything at once, let's see if we can retrieve all the pieces we'll need to construct a QA query of the kind we're looking for, one step at a time. Let's start with the metadata.

### 4a. Get the Table's Metadata (table_name, column_name, ordinal_position, data_type)
Metadata is, quite literally, data about data. In this case, we're talking about retrieving data from the Athena table that we created in our ```CREATE TABLE IF NOT EXISTS``` script, not by looking directly at the data itself, but by querying the schema that's automatically stored when you create a table in Athena. You can think of the schema as the blueprint of how data from one table relates to data that may or may not be found in data within another.

If at this point you're wondering why you would ever want to know about the data's data, you're asking the right question. Metadata is super helpful for doing QA work. For the purpose of automating it, you can think of this as a way for our code to automatically tell us things like the names of tables that are stored in our database, the names of columns that are stored in our tables, how many columns a table contains, even what datatypes a particular column may contain. By exploring the metadata of an Athena table, we can understand the contents of the data much faster than we could by opening it and trying to look at all of it manually.

To retrieve metadata for a table that has been created using AWS Athena you can query the table's information_schema, specifying the ```table_name``` and ```table_schema``` in the query's ```WHERE``` statement, thus:
```
SELECT * 
FROM information_schema
WHERE table_schema = 'default'
AND table_name='top_spotify_songs_2023_table'
```

To retrieve information exclusively about the *columns* of a table, try:
```
SELECT * 
FROM information_schema.columns
WHERE table_schema = 'default'
AND table_name='top_spotify_songs_2023_table'
```

For the purposes of our QA procedure, we can specify the metadata we'll be interested in&mdash;namely, table_name, column_name, ordinal_position, and data_type:

```
SELECT distinct
table_name
, column_name
, ordinal_position
, data_type
FROM information_schema.columns
WHERE table_schema = 'default'
and table_name = top_spotify_songs_2023_table'
order by ordinal_position
```
The output of this query should resemble the following:

| **table_name**                 | **column_name**          | **ordinal_position** | **data_type** |
|--------------------------------|--------------------------|----------------------|---------------|
| top_spotify_songs_20203_table  | track_name               | 1                    | varchar       |
| top_spotify_songs_20203_table  | artist_name              | 2                    | varchar       |
| top_spotify_songs_20203_table  | artist_count             | 3                    | varchar       |
| top_spotify_songs_20203_table  | released_year            | 4                    | varchar       |
| top_spotify_songs_20203_table  | released_month           | 5                    | varchar       |
| top_spotify_songs_20203_table  | released_day             | 6                    | varchar       |
| top_spotify_songs_20203_table  | in_spotify_playlists     | 7                    | varchar       |           
| top_spotify_songs_20203_table  | in_spotify_charts        | 8                    | varchar       |
| top_spotify_songs_20203_table  | streams                  | 9                    | varchar       |
| top_spotify_songs_20203_table  | in_apple_playlists       | 10                   | varchar       |
| top_spotify_songs_20203_table  | in_apple_charts          | 11                   | varchar       |
| top_spotify_songs_20203_table  | in_deezer_playlists      | 12                   | varchar       |
| top_spotify_songs_20203_table  | in_deezer_charts         | 13                   | varchar       |
| top_spotify_songs_20203_table  | in_shazam_charts         | 14                   | varchar       |
| top_spotify_songs_20203_table  | bpm                      | 15                   | varchar       |
| top_spotify_songs_20203_table  | key                      | 16                   | varchar       |
| top_spotify_songs_20203_table  | mode                     | 17                   | varchar       |
| top_spotify_songs_20203_table  | danceability_percent     | 18                   | varchar       |
| top_spotify_songs_20203_table  | valence_percent          | 19                   | varchar       |
| top_spotify_songs_20203_table  | energy_percent           | 20                   | varchar       |
| top_spotify_songs_20203_table  | acousticness_percent     | 21                   | varchar       |
| top_spotify_songs_20203_table  | instrumentalness_percent | 22                   | varchar       |
| top_spotify_songs_20203_table  | liveness_percent         | 23                   | varchar       |
| top_spotify_songs_20203_table  | speechiness_percent      | 24                   | varchar       |


### 4b. Get the Count of Null Values in a Column

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
        )
SELECT 
count(col1) null_count 
FROM table_data 
WHERE col1 is null 
or col1 like '' 
or col1 like ' '
```
The output of the above should resemble the following:

| **null_count** |
|----------------|
| 0              |

### 4c. Get the Count of Non-Null and Distinct Values in a Column

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
        )
SELECT 
count(col1) total_nonnull_count
, count(distinct col1) distinct_count
FROM table_data
WHERE col1 is not null
and col1 <> ''
and col1 <> ' '
```
The output of the above should resemble the following:

| **total_nonnull_count** | **distinct_count** |
|-------------------------|--------------------|
| 953                     | 943                | 

### 4d. Create a Flag to Indicate Whether the Column Data Contains Duplicates
By 'flag' here, I mean a 'Y' or a 'N' to indicate either:
- Y = "Yes, the column contains duplicate values."
- N = "No, all the values in the column are distinct."

Flags can be helpful for guiding further QA work (for instance, knowing there are duplicates within a dataset might prompt us to ask, as a follow up, "Which rows/values are duplicated...and why?"). They can also be extremely useful for developing a more complicated code. By storing information (such as whether a dataset contains duplicates or not) in a flag, we now have an easy way to get our query to do more or less, based on the outcome of that information.

This complexity will come into play in later steps. For now let's determine the logic for discovering whether our column of data contains duplicate values.

Already we can use code we've already written to help us determine whether our column contains duplicates. If we know the total count of our non-null dataset as well as the distinct count, it stands to reason that if the total is greater than the distinct count, duplicates exist.

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
        )
SELECT
a.total_nonnull_count
, a.distinct_count
, CASE WHEN a.total_nonnull_count > a.distinct_count THEN 'Y' ELSE 'N' END contains_duplicates
FROM (
    SELECT count(col1) total_nonnull_count
    , count(distinct col1) distinct_count 
    FROM table_data
    ) a
WHERE col1 is not null
and col1 <> '' 
and col1 <> ' '
```
The output of the above should resemble the following:

| **total_nonnull_count** | **distinct_count** | **contains_duplicates** |
|-------------------------|--------------------|-------------------------|
| 953                     | 943                | Y                       |

### 4e. Create a Flag to Indicate Whether the Column Data Contains Non-Alphanumeric Characters
Flags can also be useful for helping determine the appropriateness of a datatype assignment to a particular column of data. In our example dataset, recall that we've called all of our data columns into Athena as **string** datatypes. Perhaps this is appropriate for our first column, '**track_name**', but consider another column like '**artist_count**' which appears (at first glance) to only contain numbers. Such a column might be more appropriately called into Athena as an **integer**, as it would allow us to perform calculations on the data in that column without having to cast the data from a string into an integer first. 

Flags that can look at all the data in a column and detect unformity can help us determine whether certain columns might be eligible for being recast as another type of data. The usefulness of knowing whether a data column contains non-alphanumeric characters is an important part of this consideration. 

Let's try to define some of what we mean when we say 'non-alphanumeric characters.' This could include exclamation points(!), 'at' symbols (@), pound signs (#), dollar signs($), percent signs (%), carrots(^), ampersands (&), asterisks(*), mathematical and logical operators (- / | + < >), as well as other punctuation marks like single and double quotation marks('' ""), backticks(``), apostrophes('), commas(,), dashes(&mdash;), hypens(-),underscores(_), periods/decimals(.) and question marks(?).

One of the best ways to determine whether a particular character is found in a dataset is to use regular expressions. Athena has a function called ```regexp_like()``` that will return True or False based on a regular expression input.

Rather than putting the ```regexp_like()``` function in a case statement and attempting to determine the output, we can create a count of the number of 'True' returns, and use the returned count as the basis for whether our flag should be 'Y' or 'N'.

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
        )
SELECT
CASE WHEN non_alphanumeric_count > 0 THEN 'Y' ELSE 'N' END contains_non_alphanumerics
FROM (
    SELECT count(col1) non_alphanumeric_count
    FROM table_data
    WHERE regexp_like(col1, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\'|\"|\_|\|\')=True
)
```
The output of the above should resemble the following:

| **contains_non_alphanumerics** |
|--------------------------------|
| Y                              |

### 4f. Create a Flag to Indicate Whether the Column Data Contains Letters
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
        )
SELECT 
CASE WHEN contains_letters_count > 0 THEN 'Y' ELSE 'N' END contains_letters
FROM (
    SELECT count(col1) contains_letters_count
    FROM table_data
    WHERE regexp_like(col1, '[A-Za-z]')=True
)
```
The output of the above should resemble the following:

| **contains_letters** |
|----------------------|
| Y                    |

### 4g. Get the Minimum and Maximum Value of the Number of Characters Contained in the Column's Non-Null Entries
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
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
The output of the above should resemble the following:

| **min_charlength** | **max_charlength** |
|--------------------|--------------------|
| 2                  | 123                |

### 4h. Get the Alphabetical Minimum and Maximum Value of the Data in the Column's Non-Null Entries
This metric will present us with actual data points. Alphabetical max and min are essentially the first and last items from a dataset when the column is read as a string and sorted alphabetically. If an item in such a column contains only numbers, Athena will order it before entries that begin with letters of the alphabet. It will also order numbers by their first digit, so it would read '9' as being higher in precedence than '8000'.

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
        )
SELECT 
min(col1) alphabetical_min
, max(col1) alphabetical_max
FROM table_data
WHERE col1 is not null and col1 <> '' and col1 <> ' '
```
The output of the above should resemble the following:

| **alphabetical_min** | **alphabetical_max** |
|----------------------|----------------------|
| 10:35                | ZOOM                 |

### 4i. Create a Flag to Indicate Whether the Column Data Contains Numbers
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
        )
SELECT 
CASE WHEN contains_numbers_count > 0 THEN 'Y' ELSE 'N' END contains_numbers
FROM (
    SELECT count(col1) contains_numbers_count
    FROM table_data
    WHERE regexp_like(col1, '\d')=True
)
```
The output of the above should resemble the following:

| **contains_numbers** |
|----------------------|
| Y                    |

### 4j. Create a Flag to Indicate Whether the Column Data Contains ONLY Numbers
For this flag we can utilize some of the flags we've already created. If our contains_numbers flag is 'Y' and our contains_letters flag is 'N' and our contains_non_alphanumerics flag is also 'N' then it's likely that our column contains only numbers. If a column returns a 'Y' for this field, it's likely that the column could be recast as an integer datatype. 
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
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
The output of the above should resemble the following:

| **contains_only_numbers** |
|---------------------------|
| N                         |

### 4k. Get the Numeric Minimum and Maximum Value of the Data in the Column's Non-Null Entries, If Applicable
For this metric, we want to find the maximum and minimum values of the column only if the column contains numeric data; in other words, what is the maximum and minumum value of the data if it were cast to an integer datatype. For columns like the first column in our dataset that are primarily alphanumeric, the attempt to cast any of the values to an integer datatype is going to fail. This is why we use the ```try()``` function in the code below. If the data cannot be cast to an integer datatype, the ```try()``` function will merely return a NULL value, rather than erroring out.

Additionally, because this metric may not apply to all the columns (as it doesn't to the first column of our dataset), we want to recast the minimum and maximum values as varchars in our ```CASE``` statement so that, in the case that the value is null, we can return 'N/A' as the query result. In Athena, CASE statements connot return values belonging to different datatypes; if 'N/A' is among the possible returns, then any other value that is returned must also be a varchar.

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
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
The output of the above should resemble the following:

| **numeric_max** | **numeric_min** |
|-----------------|-----------------|
| N/A             | N/A             |

### 4l. Create a Flag to Indicate Whether the Column Data Contains Decimals
While this information would already be caught by the flag we created to locate non-alphanumeric characters, isolating the decimal from other non-alphanumerics can be useful for determining whether the data found in this column could potentially be re-classified as a decimal datatype.

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
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
The output of the above should resemble the following:

| **contains_decimals** | 
|-----------------------|
| Y                     |

### 4m. Create a Flag to Indicate Whether the Column Data Could Be Cast as a Decimal DataType
As we can see from the previous flag, the fact that a column contains decimals is not enough information to determine whether the column could be recast as a decimal datatype. For the latter to be plausible, we need to confirm that decimals are present in every non-null datarow. The following code achieves this:

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
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
The output of the above should resemble the following:

| **decimal_datatype_flag** | 
|---------------------------|
| N                         |

### 4n. Get the Minimum and Maximum Value of the Data, Cast as Decimals, in the Column's Non-Null Entries, If Applicable
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
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
The output of the above should resemble the following:

| **decimal_max** | **decimal_min** | 
|-----------------|-----------------|
| N/A             | N/A             |

### 4o. Create an Indicator to Describe the Format of the Date, If Applicable
To capture data that could be intended as dates (numerically speaking), we want to consider all the ways dates could be written.
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
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
The output of the above should resemble the following:

| **date_format_indicator** | 
|---------------------------|
| N/A                       |

### 4p. Create a Flag to Indicate Whether the Column Data Could Be Interpreted as a Date

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
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
The output of the above should resemble the following:

| **date_flag** | 
|---------------|
| N/A           |

### 4q. Get the Minimum and Maximum Value of the Data, Interpreted as a Date DataType, If Applicable
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
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
The output of the above should resemble the following:

| **max_date** | **min_date** | 
|--------------|--------------|
| N/A          | N/A          |

# Step 5 : Tie all the Queries From Step 4 Together Into a Single Continuous Query ('col1') That Will Perform All of the QA Procedures Outlined in Step 4 on A Single Column of Your Table's Data

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
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
The output of the above should resemble the following:

| **table_name**                 | **column_name**          | **ordinal_position** | **data_type** |
|--------------------------------|--------------------------|----------------------|---------------|
| top_spotify_songs_20203_table  | track_name               | 1                    | varchar       |

| **temp_column_name** | **null_count** | **total_nonnull_count** | **distinct_count** | **contains_duplicates** |
|----------------------|----------------|-------------------------|--------------------|-------------------------|
| col1                 | 0              | 953                     | 943                | Y                       |

| **contains_non_alphanumerics** | **contains_letters** | **min_charlength** | **max_charlength** |
|--------------------------------|----------------------|--------------------|--------------------|
| Y                              | Y                    | 2                  | 123                |

| **alphabetical_min** | **alphabetical_max** | **contains_numbers** | **contains_only_numbers** |
|----------------------|----------------------|----------------------|---------------------------|
| 10:35                | ZOOM                 | Y                    | N                         |

| **numeric_max** | **numeric_min** | **contains_decimals** | **decimal_datatype_flag** |
|-----------------|-----------------|-----------------------|---------------------------|
| N/A             | N/A             | Y                     | N                         |

| **decimal_max** | **decimal_min** | **date_format_indicator** | **date_flag** | **max_date** | **min_date** |
|-----------------|-----------------|---------------------------|---------------|--------------|--------------|
| N/A             | N/A             | N/A                       | N/A           | N/A          | N/A

# Step 6 : Duplicate The Col1 Query from Step 5 For the First 10 Columns of Your Table
To duplicate the Col1 script to be applicable to additonal columns, I recommend copying and pasting the above query into a new query window in your Athena query editor. Once there, you can change the 'col1' variable to 'col2' everywhere it appears in the query using Athena's FIND & REPLACE function. To summon the FIND & REPLACE menu, click anywhere in the query editor window and hit 'Ctrl + F' to open the FIND & REPLACE window. Once open, you can select the '+' sign to open the 'REPLACE' window. Enter 'col1' in the FIND searchbar and 'col2' in the REPLACE WITH bar and select 'REPLACE ALL'.

Once the 'col1' variable has been replaced with 'col2', highlight the entire script and paste it back into the original query, directly below your col1 query from Step 5. 

Repeat the above process until your query resembles the code below:

> NOTE: The code below will not actually run if copied and pasted into your query editor. The lines of code that begin with 'col' names are meant to represent the structure of what your code should look like. They are written this way to conserve space in this repository.
V V V V V
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
        )
, metadata as (
    SELECT distinct
    table_name
    , column_name
    , ordinal_position
    , data_type
    FROM information_schema.columns
    WHERE table_schema='default'
    AND table_name = top_spotify_songs_2023_table'
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
At this point you may be wondering why we've only opted to query the first 10 columns of our data? Why not do all 28 columns at once?

The reason is becuase of Athena's query limitations. The maximum allowed query string length is 262144 bytes, where the strings are encoded in UTF-8.

When our col1 Query from STEP 5 is copied ten times, our code exceeds 152,000 characters and occupies nearly 3600 lines of code. Athena can handle this amount, but try much more and you may get an error message.

To query all 28 columns of our dataset, my recommendation would be to write two separate queries: the first querying columns #1-14, the second querying columns #15-28. Depending on the size of your dataset, this amount may have to change. Anecdotally, I'll share that I've been successful querying up to 10 columns at a time from datasets that contain upwards of 17 million records.

[Click here to read more about AWS Query Service Quotas](https://docs.aws.amazon.com/athena/latest/ug/service-limits.html).

# Step 7 : Rewrite the Col1_str() Query From Step 5 To Handle Integer Datatypes
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
        )
, metadata as (
    SELECT distinct
    table_name
    , column_name
    , ordinal_position
    , data_type
    FROM information_schema.columns
    WHERE table_schema='default'
    AND table_name = top_spotify_songs_2023_table'
    ORDER BY ordinal_position
)
, metadata2 as (
    SELECT m.*
    , concat('col', cast(m.ordinal_position as varchar(3))) temp_column_name
    FROM metadata m
)
, col2_int as (
SELECT
md.table_name, md.column_name, md.ordinal_position, md.data_type, md.temp_column_name
, b.null_count, total_nonnull_count, distinct_count
, CASE WHEN total_nonnull_count > distinct_count then 'Y' ELSE 'N' END contains_duplicates
, 'N' contains_non_alphanumerics
, 'N' contains_letters
, min_charlength, max_charlength, alphabet_min, alphabet_max
, 'Y' contains_numbers
, 'Y' contains_only_numbers
, numeric_min, numeric_max
, 'N' contains_decimals
, 'N' decimal_datatype_flag
, 'N/A' decimal_max
, 'N/A' decimal_min
, 'N/A' date_flag
, 'N/A' date_form
, 'N/A' max_date
, 'N/A' min_date
FROM (
    SELECT
    count(col2) total_nonnull_count
    , count(distinct col2) distinct_count
    , min(length(cast(col2 as varchar))) min_charlength
    , max(length(cast(col2 as varchar))) max_charlength
    , min(cast(col2 as varchar)) alphabet_min
    , max(cast(col2 as varchar)) alphabet_max
    , cast(min(col2 as varchar)) numeric_min
    , cast(max(col2 as varchar)) numeric_max
    FROM table_data
    WHERE col2 is not null and cast(col2 as varchar) <> '' and cast(col2 as varchar) <> ' '
    )
    FULL OUTER JOIN (
    SELECT total_count-non_null_count null_count
    FROM (
        SELECT 
        count(*) total_count
        , a.non_null_count
        FROM table_data
        FULL OUTER JOIN (
            SELECT count(col2) non_null_count
            FROM table_data where regexp_like(cast(col2 as varchar), '\d')
            ) a
        ON 1=1
        GROUP BY a.non_null_count
        ) b
    )
    ON 1=1
    FULL OUTER JOIN metadata2 md
    ON 1=1
    WHERE md.temp_column_name = 'col2'
)
SELECT * FROM col2_int
```
# Step 8 : Rewrite the Col1_str Query From Step 5 To Handle Decimal Datatypes
```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
        )
, metadata as (
    SELECT distinct
    table_name
    , column_name
    , ordinal_position
    , data_type
    FROM information_schema.columns
    WHERE table_schema='default'
    AND table_name = top_spotify_songs_2023_table'
    ORDER BY ordinal_position
)
, metadata2 as (
    SELECT m.*
    , concat('col', cast(m.ordinal_position as varchar(3))) temp_column_name
    FROM metadata m
)
, col3_dec as (
    SELECT
    md.table_name, md.column_name, md.ordinal_position, md.data_type, md.temp_column_name
    , null_count, total_nonnull_count, distinct_count
    , CASE when total_nonnull_count <> distinct_count THEN 'Y' ELSE 'N' end as contains_duplicates
    , contains_non_alphanumerics, contains_letters
    , min_charlength, max_charlength
    , alphabet_min, alphabet_max
    , contains_numbers, contains_only_numbers
    , CASE 
        WHEN numeric_max is null THEN split_part(cast(decimal_max as varchar),'.',1)
        ELSE numeric_max END numeric_max
    , CASE
        WHEN numeric_min is null THEN split_part(cast(decimal_min as varchar),'.',1)
        ELSE numeric_min END numeric_min
    , contains_decimals, decimal_datatype_flag, decimal_max, decimal_min, date_flag, date_form
    , max_date, min_date
    FROM (
        SELECT 
        b.null_count
        , count(col3) total_nonnull_count
        , count(distinct col3) distinct_count
        , 'Y' contains_non_alphanumerics
        , length(min(try(cast(col3 as varchar)))) min_charlength
        , length(max(try(cast(col3 as varchar)))) max_charlength
        , min(try(cast(col3 as varchar))) alphabet_min
        , max(try(cast(col3 as varchar))) alphabet_max
        , 'Y' contains_numbers
        , 'N' contains_only_numbers
        , cast(max(try(cast(col3 as int))) as varchar) numeric_max
        , cast(min(try(cast(col3 as int))) as varchar) numeric_min
        , 'Y' contains_decimals
        , 'Y' decimal_datatype_flag
        , cast(max(col3) as varchar) decimal_max
        , cast(min(col3) as varchar) decimal_min
        , 'N' date_flag
        , 'N/A' date_form
        , 'N/A' max_date
        , 'N/A' min_date

        FROM table_data
        FULL OUTER JOIN (
            SELECT
            total_count-non_null_count null_count
            FROM (
            SELECT
            count(*) total_count
            , a.non_null_count
            FROM table_data
            FULL OUTER JOIN (
                SELECT
                count(col3) non_null_count
                FROM table_data
                WHERE regexp_like(cast(col3 as varchar), '\d')
                ) a
            ON 1=1
            WHERE col3 is not null and cast(col3 as varchar) <> '' and cast(col3 as varchar) <> ' '
            GROUP BY a.non_null_count
            ) b
        )
    )
    JOIN metadata2 md 
    ON 1=1
    WHERE md.temp_column_name = 'col3'
)
```
# Step 9 : Arrange and Modify the Query to Suit Your Dataset

Now that we have developed QA code that will handle string, integer and decimal datatypes, we can integrate these altogether into the same query. The following is the full code for a 3-column table in which the first column is a **string** datatype, the second column is an **integer** datatype, and the third column is a **decimal** datatype. 

```
WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24
    ) as (
        SELECT * FROM top_spotify_songs_2023_table
        )
, metadata as (
    SELECT distinct
    table_name
    , column_name
    , ordinal_position
    , data_type
    FROM information_schema.columns
    WHERE table_schema='default'
    AND table_name = top_spotify_songs_2023_table'
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
, col2_int as (
    SELECT
    md.table_name, md.column_name, md.ordinal_position, md.data_type, md.temp_column_name
    , b.null_count, total_nonnull_count, distinct_count
    , CASE WHEN total_nonnull_count > distinct_count then 'Y' ELSE 'N' END contains_duplicates
    , 'N' contains_non_alphanumerics
    , 'N' contains_letters
    , min_charlength, max_charlength, alphabet_min, alphabet_max
    , 'Y' contains_numbers
    , 'Y' contains_only_numbers
    , numeric_min, numeric_max
    , 'N' contains_decimals
    , 'N' decimal_datatype_flag
    , 'N/A' decimal_max
    , 'N/A' decimal_min
    , 'N/A' date_flag
    , 'N/A' date_form
    , 'N/A' max_date
    , 'N/A' min_date
    FROM (
        SELECT
        count(col2) total_nonnull_count
        , count(distinct col2) distinct_count
        , min(length(cast(col2 as varchar))) min_charlength
        , max(length(cast(col2 as varchar))) max_charlength
        , min(cast(col2 as varchar)) alphabet_min
        , max(cast(col2 as varchar)) alphabet_max
        , cast(min(col2 as varchar)) numeric_min
        , cast(max(col2 as varchar)) numeric_max
        FROM table_data
        WHERE col2 is not null and cast(col2 as varchar) <> '' and cast(col2 as varchar) <> ' '
        )
        FULL OUTER JOIN (
        SELECT total_count-non_null_count null_count
        FROM (
            SELECT 
            count(*) total_count
            , a.non_null_count
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col2) non_null_count
                FROM table_data where regexp_like(cast(col2 as varchar), '\d')
                ) a
            ON 1=1
            GROUP BY a.non_null_count
            ) b
        )
        ON 1=1
        FULL OUTER JOIN metadata2 md
        ON 1=1
        WHERE md.temp_column_name = 'col2'
)
, col3_dec as (
    SELECT
    md.table_name, md.column_name, md.ordinal_position, md.data_type, md.temp_column_name
    , null_count, total_nonnull_count, distinct_count
    , CASE when total_nonnull_count <> distinct_count THEN 'Y' ELSE 'N' end as contains_duplicates
    , contains_non_alphanumerics, contains_letters
    , min_charlength, max_charlength
    , alphabet_min, alphabet_max
    , contains_numbers, contains_only_numbers
    , CASE 
        WHEN numeric_max is null THEN split_part(cast(decimal_max as varchar),'.',1)
        ELSE numeric_max END numeric_max
    , CASE
        WHEN numeric_min is null THEN split_part(cast(decimal_min as varchar),'.',1)
        ELSE numeric_min END numeric_min
    , contains_decimals, decimal_datatype_flag, decimal_max, decimal_min, date_flag, date_form
    , max_date, min_date
    FROM (
        SELECT 
        b.null_count
        , count(col3) total_nonnull_count
        , count(distinct col3) distinct_count
        , 'Y' contains_non_alphanumerics
        , length(min(try(cast(col3 as varchar)))) min_charlength
        , length(max(try(cast(col3 as varchar)))) max_charlength
        , min(try(cast(col3 as varchar))) alphabet_min
        , max(try(cast(col3 as varchar))) alphabet_max
        , 'Y' contains_numbers
        , 'N' contains_only_numbers
        , cast(max(try(cast(col3 as int))) as varchar) numeric_max
        , cast(min(try(cast(col3 as int))) as varchar) numeric_min
        , 'Y' contains_decimals
        , 'Y' decimal_datatype_flag
        , cast(max(col3) as varchar) decimal_max
        , cast(min(col3) as varchar) decimal_min
        , 'N' date_flag
        , 'N/A' date_form
        , 'N/A' max_date
        , 'N/A' min_date

        FROM table_data
        FULL OUTER JOIN (
            SELECT
            total_count-non_null_count null_count
            FROM (
            SELECT
            count(*) total_count
            , a.non_null_count
            FROM table_data
            FULL OUTER JOIN (
                SELECT
                count(col3) non_null_count
                FROM table_data
                WHERE regexp_like(cast(col3 as varchar), '\d')
                ) a
            ON 1=1
            WHERE col3 is not null and cast(col3 as varchar) <> '' and cast(col3 as varchar) <> ' '
            GROUP BY a.non_null_count
            ) b
        )
    )
    JOIN metadata2 md 
    ON 1=1
    WHERE md.temp_column_name = 'col3'
)
, combined as (
    SELECT * from col1_str
    UNION SELECT * FROM col2_int
    UNION SElECT * FORM col3_dec
)
SELECT * FROM combined ORDER BY ordinal_position

```
Using the above, you can now construct your own QA queries to fit the datatypes and number of columns pertinent to your own dataset.

# Limitations of This Query
This query, obviously, requires many lines of code to perform relatively simple operations. The limitation this code is likely to run up against lies not with the amount of data it is being asked to process, but with the physical length of the code itself. 

The maximum allowed query string length is 262144 bytes, (which translateds to roughly 131,072 words), where the strings are encoded in UTF-8.

[Click here to read more about AWS Query Service Quotas](https://docs.aws.amazon.com/athena/latest/ug/service-limits.html).

# How To Use This Query

