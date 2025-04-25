# This is a Web Scraping ETL Project in Python
This project uses a python script to perform web scraping and ETL process. 
This project is the final project assignment of the edX's IBM **Python for Data Engineering Project** course.

**Extract:** The script extracts the top ten largest banks in the world and their market cap in USD information using web scraping. 

**Transform:** The script then transforms that market cap data by processing it to also have the market cap information in GBP, EUR and INR. 

**Load:** After that, the script loads the data into a CSV file as well as into a database. 

After the ETL process the script runs some queries into the database. The entire ETL process is logged and timestamped through the log_progress() function. 

## Libraries Used: 
- **BeautifulSoup** - for web scraping.
- **Pandas** - for transforming the scraped data into a DataFrame and adding more processed data columns.
- **Numpy** - for the data processing and conversion.
- **SQLite3** - for loading the data into a database.  

