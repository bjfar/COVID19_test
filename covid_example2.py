"""COVID19 daily deaths to SQL table test code"""

import numpy as np
import pandas as pd
import mysql.connector
from datetime import datetime

# For better table viewing
pd.set_option("display.max_rows", None, "display.max_columns", None)

# COVID19 time series data from 
# https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv
# Retrieved 3 April 2020
input_data = "time_series_covid19_deaths_global_3Apr2020.csv"
df = pd.read_csv(input_data)

# Note, some countries broken down into sub-regions, e.g.
# e.g. print(df.loc[df['Country/Region'] == 'Australia'])

# We only want whole country data, I think, so let's combine those together
# First drop some columns that we don't need and don't make sense to combine
df.drop(['Province/State','Lat','Long'],axis=1,inplace=True)
dcomb = df.groupby('Country/Region').apply(lambda dfx: dfx.sum(numeric_only=True))

# After the grouping, the Country/Region is now the dataframe index
# e.g. print(dcomb.loc['Australia'])

# This data is the cumulative deaths. We also want the change in deaths each day
# (i.e. how many people died each day)
# Luckily the columns are ordered nicely to make this easy
ddiff = dcomb.diff(axis=1)
# First date will be all NaNs. But this is fine. Cannot say how many people died each day from this data

# Upload the results to SQL tables
mydb = mysql.connector.connect(
  host="localhost",
  user="farmer",
  passwd="testpass",
  database='test_database'
)

mycursor = mydb.cursor()

# Tables to be created
all_tables = ['deaths total', 'deaths change python', 'deaths change sql']

# If tables we want already exist, delete them
for x in all_tables:
  mycursor.execute("DROP TABLE IF EXISTS `{0}`".format(x))

# The job ad is not super clear, but I think you want the data split up so that
# there are separate records for every day? This makes sense if new data is e.g.
# getting added every day, so we don't have to constantly add new columns to
# the table.
 
# Create the tables we want
stmt = "CREATE TABLE `{table}` (`record_id` INT AUTO_INCREMENT PRIMARY KEY, `country/region` VARCHAR(255), `date` DATE, `{death_col}` INT)"
mycursor.execute(stmt.format(table="deaths total", death_col="cumulative deaths"))
mycursor.execute(stmt.format(table="deaths change python", death_col="new deaths"))
mycursor.execute(stmt.format(table="deaths change sql"   , death_col="new deaths"))

# Insert some records. First for the cumulative data
table_specs = [(dcomb, "deaths total", "cumulative deaths"),
               (ddiff, "deaths change python", "new deaths")]

for df, table, deaths_col in table_specs:
    # Prepare the data for single executemany, for speedy insertion
    data = []
    for index, row in df.iterrows():
        for date, deaths in row.iteritems():
            # Need to reformat the date for mysql
            sql_date = datetime.strptime(date, '%m/%d/%y').strftime('%Y-%m-%d')
            # NaN deaths need to be set to NULL (None in Python)
            if pd.isna(deaths): deaths = None
            data += [(index, sql_date, deaths)]
    insert_cmd = "INSERT INTO `{0}` (`country/region`, `date`, `{1}`) VALUES (%s, %s, %s)".format(table,deaths_col)
    mycursor.executemany(insert_cmd, data)

# See if it worked
#mycursor.execute("SELECT `country/region`, `date`, `cumulative deaths` FROM `deaths total`")
#mycursor.execute("SELECT `country/region`, `date`, `new deaths` FROM `deaths change python`")
#for x in mycursor:
#   print(x)
#Looks good!

# Final bit: recompute the deaths each day directly from the `deaths total` SQL table
# query = "\
#          INSERT INTO `deaths change sql` \
#          SELECT \
#            `country/region`, \
#            `date`, \
#            `cumulative deaths` - LAG(`cumulative deaths`,1,0)\
#               OVER (PARTITION BY `country/region` ORDER BY `date`) AS `new deaths`\
#          FROM `deaths total`"
# Ah damn, my personal MySQL installation is too old to use these cool new window functions
# Will have to do it a more old school way.
# I guess this violates the rules since I use Python for a loop rather than pure SQL,
# but it seems to be a huge headache to do this in pure SQL without window functions.
# I looked into it for a while, can probably be done using cursors or some nested
# selection voodoo, but the following solution is infinitely easier to understand.

mycursor.execute("SELECT DISTINCT `country/region` FROM `deaths total`")
countries = [x[0] for x in mycursor]
for country in countries:
    mycursor.execute("SET @x:=NULL;")
    query = "\
             INSERT INTO `deaths change sql` \
             SELECT \
               `record_id`, \
               `country/region`, \
               `date`, \
               -(@x - @x:=`cumulative deaths`) AS `new deaths` \
             FROM `deaths total` \
             WHERE `country/region` = %(country)s \
             ORDER BY `date` \
             ;"
    mycursor.execute(query,{"country": country})

# Make sure results match between Python and SQL methods
query = "SELECT `record_id`, `country/region`, `date`, `new deaths` FROM "
mycursor.execute(query+"`deaths change python`")
python_result = [x for x in mycursor]
mycursor.execute(query+"`deaths change sql`")
sql_result = [x for x in mycursor]

for r1, r2 in zip(python_result, sql_result):
    if r1 != r2:
         print("Inconsistency detected!")
         print("  python result: ",r1)
         print("  sql result   : ",r2)
# Seems fine

# Commit everything!
mydb.commit()
