# Netflix-Data-Cleaning-and-Analysis

## This project dives into Netflix data analysis by utilizing the Kaggle API for data acquisition, building a MySQL database for storage, and implementing data cleaning techniques. It then tackles 5 specific business questions through data analysis, showcasing expertise in data acquisition, database management, data cleaning, and exploratory analysis.
![Screenshot 2024-06-02 182441](https://github.com/PRANAV7389/Netflix-Data-Cleaning-and-Analysis/assets/110465335/900cfabc-3836-48aa-91eb-5a8426e1f8b3)

# Data Pipeline to Download, Clean, and Load Data into SQL Database

## This Python script demonstrates a data pipeline that downloads a dataset from Kaggle,  prepared data into a SQL database and then SQL is used for data cleaning, and loads the  and also for further analysis. Below are the detailed steps and the corresponding code.

### 1. Download the Dataset from Kaggle



```python
import kaggle
# Download the 'orders.csv' file from the specified Kaggle dataset
!kaggle datasets download shivamb/netflix-shows -f netflix_titles.csv
```

    Dataset URL: https://www.kaggle.com/datasets/shivamb/netflix-shows
    License(s): CC0-1.0
    Downloading netflix_titles.csv.zip to C:\Users\DELL\Downloads
    
    

    
      0%|          | 0.00/1.34M [00:00<?, ?B/s]
     75%|#######4  | 1.00M/1.34M [00:01<00:00, 615kB/s]
    100%|##########| 1.34M/1.34M [00:01<00:00, 748kB/s]
    100%|##########| 1.34M/1.34M [00:01<00:00, 711kB/s]
    

### 2. Extract the Downloaded ZIP File



```python
import zipfile
# Extract the downloaded zip file containing the 'orders.csv'
zip_ref = zipfile.ZipFile('netflix_titles.csv.zip')
zip_ref.extractall()
zip_ref.close()
```

### 3. Load Data into a DataFrame



```python
import pandas as pd
# Read the CSV file into a pandas DataFrame
n = pd.read_csv('netflix_titles.csv')
# Display the first 20 rows of the DataFrame
df.head(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>show_id</th>
      <th>type</th>
      <th>title</th>
      <th>director</th>
      <th>cast</th>
      <th>country</th>
      <th>date_added</th>
      <th>release_year</th>
      <th>rating</th>
      <th>duration</th>
      <th>listed_in</th>
      <th>description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>s1</td>
      <td>Movie</td>
      <td>Dick Johnson Is Dead</td>
      <td>Kirsten Johnson</td>
      <td>NaN</td>
      <td>United States</td>
      <td>September 25, 2021</td>
      <td>2020</td>
      <td>PG-13</td>
      <td>90 min</td>
      <td>Documentaries</td>
      <td>As her father nears the end of his life, filmm...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>s2</td>
      <td>TV Show</td>
      <td>Blood &amp; Water</td>
      <td>NaN</td>
      <td>Ama Qamata, Khosi Ngema, Gail Mabalane, Thaban...</td>
      <td>South Africa</td>
      <td>September 24, 2021</td>
      <td>2021</td>
      <td>TV-MA</td>
      <td>2 Seasons</td>
      <td>International TV Shows, TV Dramas, TV Mysteries</td>
      <td>After crossing paths at a party, a Cape Town t...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>s3</td>
      <td>TV Show</td>
      <td>Ganglands</td>
      <td>Julien Leclercq</td>
      <td>Sami Bouajila, Tracy Gotoas, Samuel Jouy, Nabi...</td>
      <td>NaN</td>
      <td>September 24, 2021</td>
      <td>2021</td>
      <td>TV-MA</td>
      <td>1 Season</td>
      <td>Crime TV Shows, International TV Shows, TV Act...</td>
      <td>To protect his family from a powerful drug lor...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>s4</td>
      <td>TV Show</td>
      <td>Jailbirds New Orleans</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>September 24, 2021</td>
      <td>2021</td>
      <td>TV-MA</td>
      <td>1 Season</td>
      <td>Docuseries, Reality TV</td>
      <td>Feuds, flirtations and toilet talk go down amo...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>s5</td>
      <td>TV Show</td>
      <td>Kota Factory</td>
      <td>NaN</td>
      <td>Mayur More, Jitendra Kumar, Ranjan Raj, Alam K...</td>
      <td>India</td>
      <td>September 24, 2021</td>
      <td>2021</td>
      <td>TV-MA</td>
      <td>2 Seasons</td>
      <td>International TV Shows, Romantic TV Shows, TV ...</td>
      <td>In a city of coaching centers known to train I...</td>
    </tr>
  </tbody>
</table>
</div>



### 4 Load Data into SQL Database



```python
from sqlalchemy import create_engine
import sqlalchemy as sal

# Database connection details
user = 'pranav_user'
password = 'kshama1234'
host = 'localhost'
database = 'pranav'

# Create a SQLAlchemy engine
engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
conn = engine.connect()

# Load the DataFrame into the SQL database table 'df_orders'
df.to_sql('netflix_raw', con=conn, index=False, if_exists='append')
conn.close()
```

### 5 Data Exploration


```python
len(df)
```




    8807




```python
df[df.show_id=='s5023']
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>show_id</th>
      <th>type</th>
      <th>title</th>
      <th>director</th>
      <th>cast</th>
      <th>country</th>
      <th>date_added</th>
      <th>release_year</th>
      <th>rating</th>
      <th>duration</th>
      <th>listed_in</th>
      <th>description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>5022</th>
      <td>s5023</td>
      <td>Movie</td>
      <td>반드시 잡는다</td>
      <td>Hong-seon Kim</td>
      <td>Baek Yoon-sik</td>
      <td>South Korea</td>
      <td>February 28, 2018</td>
      <td>2017</td>
      <td>TV-MA</td>
      <td>110 min</td>
      <td>Dramas, International Movies, Thrillers</td>
      <td>After people in his town start turning up dead...</td>
    </tr>
  </tbody>
</table>
</div>




```python
df.show_id.str.len().max()
```




    5




```python
columns = df.columns.to_list()
```


```python
columns
```




    ['show_id',
     'type',
     'title',
     'director',
     'cast',
     'country',
     'date_added',
     'release_year',
     'rating',
     'duration',
     'listed_in',
     'description']




```python
for i in columns:
    try:
        l = max(df[i].dropna().str.len())
        print(i,l)
    except:
        l = max(df[i].dropna())
        print(i,l)
```

    show_id 5
    type 7
    title 104
    director 208
    cast 771
    country 123
    date_added 19
    release_year 2021
    rating 8
    duration 10
    listed_in 79
    description 248
    


```python
df.isna().sum()
```




    show_id            0
    type               0
    title              0
    director        2634
    cast             825
    country          831
    date_added        10
    release_year       0
    rating             4
    duration           3
    listed_in          0
    description        0
    dtype: int64



#  SQL Query to clean, transform and analyze the data:



```python
-- Create the main table to store raw Netflix data
CREATE TABLE `netflix_raw` (
  `show_id` varchar(10),
  `type` varchar(10),
  `title` varchar(200),
  `director` varchar(250),
  `cast` varchar(1000),
  `country` varchar(150),
  `date_added` varchar(20),
  `release_year` int DEFAULT NULL,
  `rating` varchar(10),
  `duration` varchar(10),
  `listed_in` varchar(100),
  `description` varchar(500)
);

-- Select all records from the raw Netflix table
SELECT * FROM netflix_raw;

-- Remove duplicates by identifying duplicate show_id entries
SELECT show_id, COUNT(*)
FROM netflix_raw
GROUP BY show_id
HAVING COUNT(*) > 1;

-- Select records with duplicate titles and types, ignoring case
SELECT *
FROM netflix_raw
WHERE CONCAT(ANY_VALUE(UPPER(title)), type) IN (
  SELECT CONCAT(ANY_VALUE(UPPER(title)), type)
  FROM netflix_raw
  GROUP BY UPPER(title), type
  HAVING COUNT(*) > 1
)
ORDER BY UPPER(title);

-- Remove duplicate records, keeping only the first occurrence for each title and type
WITH cte AS (
  SELECT *,
         ROW_NUMBER() OVER(PARTITION BY title, type ORDER BY show_id) AS rn
  FROM netflix_raw
)
SELECT * 
FROM cte
WHERE rn = 1;

-- Create a new table for storing directors
CREATE TABLE netflix_directors (
  show_id varchar(10) NOT NULL,
  director VARCHAR(255) NOT NULL
);

-- Insert unique directors into the new table
INSERT INTO netflix_directors (show_id, director)
SELECT nr.show_id, TRIM(SUBSTRING_INDEX(nr.director, ',', 1)) AS director
FROM netflix_raw nr
WHERE nr.show_id IS NOT NULL AND nr.director IS NOT NULL AND nr.director <> '';

-- Select all records from the directors table
SELECT * FROM netflix_directors;

-- Create a new table for storing countries
CREATE TABLE netflix_country (
  show_id varchar(10) NOT NULL,
  country VARCHAR(150) NOT NULL
);

-- Insert unique countries into the new table
INSERT INTO netflix_country (show_id, country)
SELECT nr.show_id, TRIM(SUBSTRING_INDEX(nr.country, ',', 1)) AS country
FROM netflix_raw nr
WHERE nr.show_id IS NOT NULL AND nr.country IS NOT NULL AND nr.country <> '';

-- Select all records from the country table
SELECT * FROM netflix_country;

-- Create a new table for storing cast members
CREATE TABLE netflix_cast (
  show_id varchar(10) NOT NULL,
  cast VARCHAR(1000) NOT NULL
);

-- Insert unique cast members into the new table
INSERT INTO netflix_cast (show_id, cast)
SELECT nr.show_id, TRIM(SUBSTRING_INDEX(nr.cast, ',', 1)) AS cast
FROM netflix_raw nr
WHERE nr.show_id IS NOT NULL AND nr.cast IS NOT NULL AND nr.cast <> '';

-- Select all records from the cast table
SELECT * FROM netflix_cast;

-- Create a new table for storing genres
CREATE TABLE netflix_genre (
  show_id varchar(10) NOT NULL,
  genre VARCHAR(100) NOT NULL
);

-- Insert unique genres into the new table
INSERT INTO netflix_genre (show_id, genre)
SELECT nr.show_id, TRIM(SUBSTRING_INDEX(nr.listed_in, ',', 1)) AS genre
FROM netflix_raw nr
WHERE nr.show_id IS NOT NULL AND nr.listed_in IS NOT NULL AND nr.listed_in <> '';

-- Select all records from the genre table
SELECT * FROM netflix_genre;

-- Remove duplicate records again, keeping only the first occurrence for each title and type
WITH cte AS (
  SELECT *,
         ROW_NUMBER() OVER(PARTITION BY title, type ORDER BY show_id) AS rn
  FROM netflix_raw
)
SELECT show_id, type, title, CAST(date_added AS DATE) AS date_added, release_year,
       rating, duration, description
FROM cte
WHERE rn = 1;

-- Update the country table with the missing country information
INSERT INTO netflix_country
SELECT nr.show_id, m.country 
FROM netflix_raw nr
INNER JOIN (
  SELECT director, country
  FROM netflix_country nc
  INNER JOIN netflix_directors nd ON nc.show_id = nd.show_id
  GROUP BY director, country
) m ON nr.director = m.director
WHERE nr.country IS NULL;

-- Select all records from the raw Netflix table where duration is null
SELECT * FROM netflix_raw WHERE duration IS NULL;

-- Create a simplified Netflix table without director, country, cast, and genre information
CREATE TABLE netflix (
  show_id varchar(10),                         
  type varchar(10),
  title varchar(200),
  date_added varchar(20),
  release_year int DEFAULT NULL,
  rating varchar(10),
  duration varchar(10),
  description varchar(500)
);

-- Insert cleaned and transformed data into the simplified Netflix table
INSERT INTO netflix (show_id, type, title, date_added, release_year, rating, duration, description)
SELECT show_id, type, title, date_added AS date, release_year,
       rating, CASE WHEN duration IS NULL THEN rating ELSE duration END AS duration, description
FROM (
  WITH cte AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY title, type ORDER BY show_id) AS rn
    FROM netflix_raw
  )
  SELECT * FROM cte
  WHERE rn = 1
) AS temp_table;

-- Select all records from the simplified Netflix table
SELECT * FROM netflix;

-- Analysis 1: Count the number of movies and TV shows for each director who has created both
SELECT nd.director, 
       COUNT(DISTINCT CASE WHEN n.type = 'Movie' THEN n.show_id END) AS no_of_movies,
       COUNT(DISTINCT CASE WHEN n.type = 'TV Show' THEN n.show_id END) AS no_of_tv_shows
FROM netflix n
INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
GROUP BY nd.director
HAVING COUNT(DISTINCT n.type) > 1;

-- Analysis 2: Find the country with the highest number of comedy movies
SELECT nc.country, COUNT(DISTINCT ng.show_id) AS no_of_movies
FROM netflix_genre ng
INNER JOIN netflix_country nc ON ng.show_id = nc.show_id
INNER JOIN netflix n ON ng.show_id = nc.show_id
WHERE ng.genre = 'Comedies' AND n.type = 'Movie'
GROUP BY nc.country
ORDER BY no_of_movies DESC;

-- Analysis 3: For each year, find the director with the maximum number of movies released
WITH cte AS (
  SELECT nd.director, YEAR(date_added) AS date_year, COUNT(n.show_id) AS no_of_movies
  FROM netflix n
  INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
  WHERE type = 'Movie'
  GROUP BY nd.director, YEAR(date_added)
),
cte2 AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY date_year ORDER BY no_of_movies DESC, director) AS rn
  FROM cte
)
SELECT * FROM cte2 WHERE rn = 1;

-- Analysis 4: Calculate the average duration of movies in each genre
SELECT ng.genre, AVG(CAST(REPLACE(duration, ' min', '') AS UNSIGNED INTEGER)) AS avg_duration
FROM netflix n
INNER JOIN netflix_genre ng ON n.show_id = ng.show_id
WHERE type = 'Movie'
GROUP BY ng.genre
LIMIT 0, 1000;

-- Analysis 5: Find directors who have created both horror and comedy movies
SELECT nd.director,
       COUNT(DISTINCT CASE WHEN ng.genre = 'Comedies' THEN n.show_id END) AS no_of_comedy,
       COUNT(DISTINCT CASE WHEN ng.genre = 'Horror Movies' THEN n.show_id END) AS no_of_horror
FROM netflix n
INNER JOIN netflix_genre ng ON n.show_id = ng.show_id
INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
WHERE type = 'Movie' AND ng.genre IN ('Comedies', 'Horror Movies')
GROUP BY nd.director
HAVING COUNT(DISTINCT ng.genre) = 2;

-- Select all genres for the specified director
SELECT * FROM netflix_genre WHERE show_id IN 
(SELECT show_id FROM netflix_directors WHERE director = 'Steve Brill')
ORDER BY genre;

-- Select distinct genres and order them
SELECT genre FROM netflix_genre GROUP BY genre ORDER BY genre;

```

