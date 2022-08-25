########Processing#######

###Q2###
import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import * 

#Daily schema
###Q2 (a)
#Define schema for daily
schema_Daily = StructType([
    StructField("ID", StringType()),
    StructField("DATE", StringType()),
    StructField("ELEMENT", StringType()),
    StructField("VALUE", DoubleType()),
    StructField("MEASUREMENT FLAG", StringType()),
    StructField("QUALITY FLAG", StringType()),
    StructField("SOURCE FLAG", StringType()),
    StructField("OBSERVATION TIME", StringType())
])


###Q2 (b)
#Load 1000 rows to see whether the data is accurate, was there anything unexpected?
daily_2022 = (
    spark.read.load("hdfs:///data/ghcnd/daily/2022.csv.gz", format = "csv", header = "false", limit = 1000, inferSchema = "false", schema = schema_Daily)
)

daily_2022.show()
# :<EOF>
# +-----------+--------+-------+-----+----------------+------------+-----------+----------------+
# |         ID|    DATE|ELEMENT|VALUE|MEASUREMENT FLAG|QUALITY FLAG|SOURCE FLAG|OBSERVATION TIME|
# +-----------+--------+-------+-----+----------------+------------+-----------+----------------+
# |AE000041196|20220101|   TAVG|204.0|               H|        null|          S|            null|
# |AEM00041194|20220101|   TAVG|211.0|               H|        null|          S|            null|
# |AEM00041218|20220101|   TAVG|207.0|               H|        null|          S|            null|
# |AEM00041217|20220101|   TAVG|209.0|               H|        null|          S|            null|
# |AG000060390|20220101|   TAVG|121.0|               H|        null|          S|            null|
# |AG000060590|20220101|   TAVG|151.0|               H|        null|          S|            null|
# |AG000060611|20220101|   TAVG|111.0|               H|        null|          S|            null|
# |AGE00147708|20220101|   TMIN| 73.0|            null|        null|          S|            null|
# |AGE00147708|20220101|   PRCP|  0.0|            null|        null|          S|            null|
# |AGE00147708|20220101|   TAVG|133.0|               H|        null|          S|            null|
# |AGE00147716|20220101|   TMIN|107.0|            null|        null|          S|            null|
# |AGE00147716|20220101|   PRCP|  0.0|            null|        null|          S|            null|
# |AGE00147716|20220101|   TAVG|133.0|               H|        null|          S|            null|
# |AGE00147718|20220101|   TMIN| 90.0|            null|        null|          S|            null|
# |AGE00147718|20220101|   TAVG|152.0|               H|        null|          S|            null|
# |AGE00147719|20220101|   TMAX|201.0|            null|        null|          S|            null|
# |AGE00147719|20220101|   PRCP|  0.0|            null|        null|          S|            null|
# |AGE00147719|20220101|   TAVG|119.0|               H|        null|          S|            null|
# |AGM00060351|20220101|   PRCP|  0.0|            null|        null|          S|            null|
# |AGM00060351|20220101|   TAVG|126.0|               H|        null|          S|            null|
# +-----------+--------+-------+-----+----------------+------------+-----------+----------------+
# only showing top 20 rows



###Q2 (c)###
##How many rows are in each metadata table? How many stations do not have a WMO ID?
#load Stations data  

stations_text = (
    spark.read.format('text')
    .load("/data/ghcnd/ghcnd-stations.txt")
   )
stations = stations_text.select(
    F.trim(F.substring(F.col('value'),1,11)).alias('ID').cast(StringType()),
    F.trim(F.substring(F.col('value'),13,8)).alias('LATITUDE').cast(DoubleType()),
    F.trim(F.substring(F.col('value'),22,9)).alias('LONGITUDE').cast(DoubleType()),
    F.trim(F.substring(F.col('value'),32,6)).alias('ELEVATION').cast(DoubleType()),
    F.trim(F.substring(F.col('value'),39,2)).alias('STATE').cast(StringType()),
    F.trim(F.substring(F.col('value'),42,30)).alias('NAME').cast(StringType()),
    F.trim(F.substring(F.col('value'),73,3)).alias('GSN FLAG').cast(StringType()),
    F.trim(F.substring(F.col('value'),77,3)).alias('HCN/CRN FLAG').cast(StringType()),
    F.trim(F.substring(F.col('value'),81,5)).alias('WMO ID').cast(StringType())
    )
stations.count()
# :<EOF>
# Out[3]: 118493
stations.filter(F.col('WMO ID') == "").count()
#Out[5]: 110407

#lad Countries data
countries_text = (spark.read.format('text')
                  .load("hdfs:///data/ghcnd/ghcnd-countries.txt"))
countries = countries_text.select(
    F.trim(F.substring(F.col('value'),1,2)).alias('CODE').cast(StringType()),
    F.trim(F.substring(F.col('value'),4,64)).alias('NAME').cast(StringType())
    )
countries.count()
#Out[6]: 219

#load States data
states_text = (spark.read.text("hdfs:///data/ghcnd/ghcnd-states.txt"))
states = states_text.select(
    F.trim(F.substring(F.col('value'),1,2)).alias('CODE').cast(StringType()),
    F.trim(F.substring(F.col('value'),4,47)).alias('NAME').cast(StringType())
    )
states.count()
Out[7]: 74


#load Inventory data
inventory_text = (spark.read.format('text')
                  .load("hdfs:///data/ghcnd/ghcnd-inventory.txt"))
inventory = inventory_text.select(
    F.trim(F.substring(F.col('value'),1,11)).alias('ID').cast(StringType()),
    F.trim(F.substring(F.col('value'),13,8)).alias('LATITUDE').cast(DoubleType()),
    F.trim(F.substring(F.col('value'),22,9)).alias('LONGITUDE').cast(DoubleType()),
    F.trim(F.substring(F.col('value'),32,4)).alias('ELEMENT').cast(StringType()),
    F.trim(F.substring(F.col('value'),37,4)).alias('FIRSTYEAR').cast(IntegerType()),
    F.trim(F.substring(F.col('value'),42,4)).alias('LASTYEAR').cast(IntegerType())
    )
inventory.count()
#Out[8]: 704963

#######Q3(a)
##parsed station code from country code
country_code = F.col('ID').substr(1,2)
station_code = stations.withColumn('CODE', country_code)
station_code.show()
# +-----------+--------+---------+---------+-----+--------------------+--------+------------+------+----+
# |         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN FLAG|HCN/CRN FLAG|WMO ID|CODE|
# +-----------+--------+---------+---------+-----+--------------------+--------+------------+------+----+
# |ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |            |      |  AC|
# |ACW00011647| 17.1333| -61.7833|     19.2|     |            ST JOHNS|        |            |      |  AC|
# |AE000041196|  25.333|   55.517|     34.0|     | SHARJAH INTER. AIRP|     GSN|            | 41196|  AE|
# |AEM00041194|  25.255|   55.364|     10.4|     |          DUBAI INTL|        |            | 41194|  AE|
# |AEM00041217|  24.433|   54.651|     26.8|     |      ABU DHABI INTL|        |            | 41217|  AE|
# |AEM00041218|  24.262|   55.609|    264.9|     |         AL AIN INTL|        |            | 41218|  AE|
# |AF000040930|  35.317|   69.017|   3366.0|     |        NORTH-SALANG|     GSN|            | 40930|  AF|
# |AFM00040938|   34.21|   62.228|    977.2|     |               HERAT|        |            | 40938|  AF|
# |AFM00040948|  34.566|   69.212|   1791.3|     |          KABUL INTL|        |            | 40948|  AF|
# |AFM00040990|    31.5|    65.85|   1010.0|     |    KANDAHAR AIRPORT|        |            | 40990|  AF|
# |AG000060390| 36.7167|     3.25|     24.0|     |  ALGER-DAR EL BEIDA|     GSN|            | 60390|  AG|
# |AG000060590| 30.5667|   2.8667|    397.0|     |            EL-GOLEA|     GSN|            | 60590|  AG|
# |AG000060611|   28.05|   9.6331|    561.0|     |           IN-AMENAS|     GSN|            | 60611|  AG|
# |AG000060680|    22.8|   5.4331|   1362.0|     |         TAMANRASSET|     GSN|            | 60680|  AG|
# |AGE00135039| 35.7297|     0.65|     50.0|     |ORAN-HOPITAL MILI...|        |            |      |  AG|
# |AGE00147704|   36.97|     7.79|    161.0|     | ANNABA-CAP DE GARDE|        |            |      |  AG|
# |AGE00147705|   36.78|     3.07|     59.0|     |ALGIERS-VILLE/UNI...|        |            |      |  AG|
# |AGE00147706|    36.8|     3.03|    344.0|     |   ALGIERS-BOUZAREAH|        |            |      |  AG|
# |AGE00147707|    36.8|     3.04|     38.0|     |  ALGIERS-CAP CAXINE|        |            |      |  AG|
# |AGE00147708|   36.72|     4.05|    222.0|     |          TIZI OUZOU|        |            | 60395|  AG|
# +-----------+--------+---------+---------+-----+--------------------+--------+------------+------+----+
# only showing top 20 rows


#######Q3(b)
##Left Join Stations with Countries

countries = countries.select(
    F.col('CODE'),
    F.col('NAME').alias('COUNTRYNAME'))
stations_joined = station_code.join(countries, on = "CODE", how = "left")
stations_joined.show()

# :<EOF>
# +----+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+
# |CODE|         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN FLAG|HCN/CRN FLAG|WMO ID|         COUNTRYNAME|
# +----+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+
# |  AC|ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |            |      | Antigua and Barbuda|
# |  AC|ACW00011647| 17.1333| -61.7833|     19.2|     |            ST JOHNS|        |            |      | Antigua and Barbuda|
# |  AE|AE000041196|  25.333|   55.517|     34.0|     | SHARJAH INTER. AIRP|     GSN|            | 41196|United Arab Emirates|
# |  AE|AEM00041194|  25.255|   55.364|     10.4|     |          DUBAI INTL|        |            | 41194|United Arab Emirates|
# |  AE|AEM00041217|  24.433|   54.651|     26.8|     |      ABU DHABI INTL|        |            | 41217|United Arab Emirates|
# |  AE|AEM00041218|  24.262|   55.609|    264.9|     |         AL AIN INTL|        |            | 41218|United Arab Emirates|
# |  AF|AF000040930|  35.317|   69.017|   3366.0|     |        NORTH-SALANG|     GSN|            | 40930|         Afghanistan|
# |  AF|AFM00040938|   34.21|   62.228|    977.2|     |               HERAT|        |            | 40938|         Afghanistan|
# |  AF|AFM00040948|  34.566|   69.212|   1791.3|     |          KABUL INTL|        |            | 40948|         Afghanistan|
# |  AF|AFM00040990|    31.5|    65.85|   1010.0|     |    KANDAHAR AIRPORT|        |            | 40990|         Afghanistan|
# |  AG|AG000060390| 36.7167|     3.25|     24.0|     |  ALGER-DAR EL BEIDA|     GSN|            | 60390|             Algeria|
# |  AG|AG000060590| 30.5667|   2.8667|    397.0|     |            EL-GOLEA|     GSN|            | 60590|             Algeria|
# |  AG|AG000060611|   28.05|   9.6331|    561.0|     |           IN-AMENAS|     GSN|            | 60611|             Algeria|
# |  AG|AG000060680|    22.8|   5.4331|   1362.0|     |         TAMANRASSET|     GSN|            | 60680|             Algeria|
# |  AG|AGE00135039| 35.7297|     0.65|     50.0|     |ORAN-HOPITAL MILI...|        |            |      |             Algeria|
# |  AG|AGE00147704|   36.97|     7.79|    161.0|     | ANNABA-CAP DE GARDE|        |            |      |             Algeria|
# |  AG|AGE00147705|   36.78|     3.07|     59.0|     |ALGIERS-VILLE/UNI...|        |            |      |             Algeria|
# |  AG|AGE00147706|    36.8|     3.03|    344.0|     |   ALGIERS-BOUZAREAH|        |            |      |             Algeria|
# |  AG|AGE00147707|    36.8|     3.04|     38.0|     |  ALGIERS-CAP CAXINE|        |            |      |             Algeria|
# |  AG|AGE00147708|   36.72|     4.05|    222.0|     |          TIZI OUZOU|        |            | 60395|             Algeria|
# +----+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+
#only showing top 20 rows

#######Q3(c)
##Left join stations and states where state_code == US

stations_states = (stations_joined
    .join(
        states
        .select(
            F.col('CODE'),
            F.col('Name').alias('STATENAME')    
        ),
    on = 'CODE', how = 'left')
)

stations_US = (stations_states
    .filter(F.col('CODE') == 'US')
    )
stations_US.show() 

# :<EOF>
# +----+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-------------+---------+
# |CODE|         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN FLAG|HCN/CRN FLAG|WMO ID|  COUNTRYNAME|STATENAME|
# +----+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-------------+---------+
# |  US|US009052008| 43.7333| -96.6333|    482.0|   SD|SIOUX FALLS (ENVI...|        |            |      |United States|     null|
# |  US|US10RMHS145| 40.5268|-105.1113|   1569.1|   CO|        RMHS 1.6 SSW|        |            |      |United States|     null|
# |  US|US10adam001|  40.568| -98.5069|    598.0|   NE|       JUNIATA 1.5 S|        |            |      |United States|     null|
# |  US|US10adam002| 40.5093| -98.5493|    601.1|   NE|     JUNIATA 6.0 SSW|        |            |      |United States|     null|
# |  US|US10adam003| 40.4663| -98.6537|    615.1|   NE|     HOLSTEIN 0.1 NW|        |            |      |United States|     null|
# |  US|US10adam004| 40.4798| -98.4026|    570.0|   NE|          AYR 3.5 NE|        |            |      |United States|     null|
# |  US|US10adam006| 40.4372| -98.5912|    601.1|   NE|     ROSELAND 2.8 SW|        |            |      |United States|     null|
# |  US|US10adam007| 40.5389| -98.4713|    588.9|   NE|    HASTINGS 5.4 WSW|        |            |      |United States|     null|
# |  US|US10adam008| 40.4953| -98.2973|    566.9|   NE|     GLENVIL 2.3 WSW|        |            |      |United States|     null|
# |  US|US10adam010| 40.5532| -98.6297|    622.1|   NE|     JUNIATA 6.9 WSW|        |            |      |United States|     null|
# |  US|US10adam011| 40.4078| -98.6161|    593.1|   NE|     ROSELAND 5.2 SW|        |            |      |United States|     null|
# |  US|US10adam012|   40.62|   -98.39|    588.9|   NE|      HASTINGS 2.3 N|        |            |      |United States|     null|
# |  US|US10adam013|   40.66|   -98.42|    598.0|   NE|    HASTINGS 5.3 NNW|        |            |      |United States|     null|
# |  US|US10adam015| 40.6258| -98.7034|    632.2|   NE|       KENESAW 2.4 W|        |            |      |United States|     null|
# |  US|US10adam016|   40.64| -98.3949|    598.0|   NE|      HASTINGS 3.7 N|        |            |      |United States|     null|
# |  US|US10adam017| 40.6567| -98.4789|    607.2|   NE|     JUNIATA 4.8 NNE|        |            |      |United States|     null|
# |  US|US10adam019| 40.6114| -98.5543|    619.0|   NE|     JUNIATA 2.9 WNW|        |            |      |United States|     null|
# |  US|US10adam022| 40.5936| -98.4299|    597.1|   NE|      HASTINGS 2.2 W|        |            |      |United States|     null|
# |  US|US10adam023| 40.5981| -98.4732|    602.0|   NE|     JUNIATA 1.8 ENE|        |            |      |United States|     null|
# |  US|US10adam024| 40.3901| -98.2715|    548.9|   NE|   BLUE HILL 6.3 ENE|        |            |      |United States|     null|
# +----+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-------------+---------+
# only showing top 20 rows


#######Q3 (d)

inventory_year = (
    inventory
    .select('ID','ELEMENT','FIRSTYEAR','LASTYEAR')
    .groupBy('ID')
    .agg({'FIRSTYEAR':'min','LASTYEAR':'max','ELEMENT':'count'}) #in the form of dictionaries
    .select(
        F.col('ID'),
        F.col('min(FIRSTYEAR)').alias('MIN_YEAR'),
        F.col('max(LASTYEAR)').alias('MAX_YEAR'),
        F.col('count(ELEMENT)').alias('NUM_ELEMENT'))
)
inventory_year.show()

# :<EOF>
# +-----------+--------+--------+-----------+
# |         ID|MIN_YEAR|MAX_YEAR|NUM_ELEMENT|
# +-----------+--------+--------+-----------+
# |ACW00011647|    1957|    1970|          7|
# |AEM00041217|    1983|    2021|          4|
# |AG000060590|    1892|    2021|          4|
# |AGE00147706|    1893|    1920|          3|
# |AGE00147708|    1879|    2021|          5|
# |AGE00147709|    1879|    1938|          3|
# |AGE00147710|    1909|    2009|          4|
# |AGE00147711|    1880|    1938|          3|
# |AGE00147714|    1896|    1938|          3|
# |AGE00147719|    1888|    2021|          4|
# |AGM00060351|    1981|    2021|          4|
# |AGM00060353|    1996|    2019|          4|
# |AGM00060360|    1945|    2021|          4|
# |AGM00060387|    1995|    2004|          4|
# |AGM00060445|    1957|    2021|          5|
# |AGM00060452|    1985|    2021|          4|
# |AGM00060467|    1981|    2019|          4|
# |AGM00060468|    1973|    2021|          5|
# |AGM00060507|    1943|    2021|          5|
# |AGM00060511|    1983|    2021|          5|
# +-----------+--------+--------+-----------+
# only showing top 20 rows


core_element = (
    inventory
    .filter(F.col('ELEMENT').isin('PRCP', 'SNOW', 'SNWD', 'TMAX', 'TMIN'))
    .groupBy('ID')
    .agg({'ELEMENT':'count'})
	.select(
        F.col('ID'),
		F.col('count(ELEMENT)').alias('NUM_CORE_ELEMENT'))
)        
core_element.count()
#Out[10]: 118407


# Count for other elements
other_element = (
    inventory
    .filter(~F.col('ELEMENT').isin('PRCP', 'SNOW', 'SNWD', 'TMAX', 'TMIN'))
    # .filter((F.col('ELEMENT') != 'PRCP') &
        # (F.col('ELEMENT') != 'SNOW') &
        # (F.col('ELEMENT')!= 'SNWD') &
        # (F.col('ELEMENT') != 'TMAX') &
        # (F.col('ELEMENT') != 'TMIN')
    # )
    .groupBy('ID')
    .agg({'ELEMENT':'count'})
    .select(
        F.col('ID'),
        F.col('count(ELEMENT)').alias('NUM_OTHER_ELEMENT'))
)
other_element.count()
#Out[12]: 83523

element_prcp = (
    inventory
    .filter((F.col('ELEMENT') == 'PRCP') 
    )
    .groupBy('ID')
    .agg({'ELEMENT':'count'})
    .select(
        F.col('ID'),
        F.col('count(ELEMENT)').alias('PRCP'))  
)       
element_prcp.count()
#Out[14]: 116455
inventory_elements = (
    inventory_year
    .join(
        core_element,
        on = 'ID',
        how = 'left' 
    )
    .join(
        other_element,
        on = 'ID',
        how = 'left'
    )
    .join(
        element_prcp,
        on='ID',
        how = 'left'
    )        
)

inventory_elements.show()

# In [18]: inventory_elements.show()
# +-----------+--------+--------+-----------+----------------+-----------------+----+
# |         ID|MIN_YEAR|MAX_YEAR|NUM_ELEMENT|NUM_CORE_ELEMENT|NUM_OTHER_ELEMENT|PRCP|
# +-----------+--------+--------+-----------+----------------+-----------------+----+
# |ACW00011647|    1957|    1970|          7|               5|                2|   1|
# |AEM00041217|    1983|    2021|          4|               3|                1|   1|
# |AG000060590|    1892|    2021|          4|               3|                1|   1|
# |AGE00147706|    1893|    1920|          3|               3|             null|   1|
# |AGE00147708|    1879|    2021|          5|               4|                1|   1|
# |AGE00147709|    1879|    1938|          3|               3|             null|   1|
# |AGE00147710|    1909|    2009|          4|               3|                1|   1|
# |AGE00147711|    1880|    1938|          3|               3|             null|   1|
# |AGE00147714|    1896|    1938|          3|               3|             null|   1|
# |AGE00147719|    1888|    2021|          4|               3|                1|   1|
# |AGM00060351|    1981|    2021|          4|               3|                1|   1|
# |AGM00060353|    1996|    2019|          4|               3|                1|   1|
# |AGM00060360|    1945|    2021|          4|               3|                1|   1|
# |AGM00060387|    1995|    2004|          4|               3|                1|   1|
# |AGM00060445|    1957|    2021|          5|               4|                1|   1|
# |AGM00060452|    1985|    2021|          4|               3|                1|   1|
# |AGM00060467|    1981|    2019|          4|               3|                1|   1|
# |AGM00060468|    1973|    2021|          5|               4|                1|   1|
# |AGM00060507|    1943|    2021|          5|               4|                1|   1|
# |AGM00060511|    1983|    2021|          5|               4|                1|   1|
# +-----------+--------+--------+-----------+----------------+-----------------+----+
# only showing top 20 rows


#How many stations that collect all 5 core elements?
stations_5_elements = inventory_elements.filter(F.col('NUM_CORE_ELEMENT') == 5)
stations_5_elements.count()
#Out[10]: 20289

#How many stations that collect only percepitation?
stations_percipitation = inventory_elements.filter((F.col('NUM_ELEMENT') == 1) & (F.col('PRCP') == 1))
stations_percipitation.count()
#Out[11]: 16136

##Q3 e
stations_inventory = (
    stations_states
    .join(
    inventory_elements, 
    on = 'ID',
    how = 'left')
)

#stations_inventory.write.parquet('hdfs:///user/bpa78/outputs/ghcnd/stations_inventory', mode = 'overwrite')

#stations_inventory.write.format('csv').mode('overwrite').save('hdfs:///user/bpa78/outputs/ghcnd/Stations')
stations_inventory.write.csv('hdfs:///user/bpa78/outputs/ghcnd/Stations', mode = 'overwrite', header = True)
#stations = spark.read.load('hdfs:///user/bpa78/outputs/ghcnd/Stations', format = "com.databricks.spark.csv", header = True)

# In [25]: stations.show()
# +-----------+----+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+---------+--------+--------+-----------+----------------+-----------------+----+
# |         ID|CODE|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN FLAG|HCN/CRN FLAG|WMO ID|         COUNTRYNAME|STATENAME|MIN_YEAR|MAX_YEAR|NUM_ELEMENT|NUM_CORE_ELEMENT|NUM_OTHER_ELEMENT|PRCP|
# +-----------+----+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+---------+--------+--------+-----------+----------------+-----------------+----+
# |AE000041196|  AE|  25.333|   55.517|     34.0| null| SHARJAH INTER. AIRP|     GSN|        null| 41196|United Arab Emirates|     null|    1944|    2021|          4|               3|                1|   1|
# |AEM00041218|  AE|  24.262|   55.609|    264.9| null|         AL AIN INTL|    null|        null| 41218|United Arab Emirates|     null|    1994|    2021|          4|               3|                1|   1|
# |AFM00040938|  AF|   34.21|   62.228|    977.2| null|               HERAT|    null|        null| 40938|         Afghanistan|     null|    1973|    2021|          5|               4|                1|   1|
# |AG000060611|  AG|   28.05|   9.6331|    561.0| null|           IN-AMENAS|     GSN|        null| 60611|             Algeria|     null|    1958|    2021|          5|               4|                1|   1|
# |AGE00147707|  AG|    36.8|     3.04|     38.0| null|  ALGIERS-CAP CAXINE|    null|        null|  null|             Algeria|     null|    1878|    1879|          3|               3|             null|   1|
# |AGE00147715|  AG|   35.42|   8.1197|    863.0| null|             TEBESSA|    null|        null|  null|             Algeria|     null|    1879|    1938|          3|               3|             null|   1|
# |AGE00147780|  AG|   37.08|     6.47|    195.0| null|SKIKDA-CAP BOUGAR...|    null|        null|  null|             Algeria|     null|    1931|    1938|          2|               2|             null|null|
# |AGE00147794|  AG|   36.78|      5.1|    225.0| null|   BEJAIA-CAP CARBON|    null|        null|  null|             Algeria|     null|    1926|    1938|          2|               2|             null|null|
# |AGM00060402|  AG|  36.712|     5.07|      6.1| null|             SOUMMAM|    null|        null| 60402|             Algeria|     null|    1973|    2021|          5|               4|                1|   1|
# |AGM00060405|  AG|    36.5|    7.717|    111.0| null|          BOUCHEGOUF|    null|        null| 60405|             Algeria|     null|    2002|    2004|          4|               3|                1|   1|
# |AGM00060415|  AG|  36.317|    3.533|    748.0| null|          AIN-BESSAM|    null|        null| 60415|             Algeria|     null|    2003|    2021|          5|               4|                1|   1|
# |AGM00060430|  AG|    36.3|    2.233|    721.0| null|             MILIANA|    null|        null| 60430|             Algeria|     null|    1957|    2021|          5|               4|                1|   1|
# |AGM00060437|  AG|  36.283|    2.733|   1036.0| null|               MEDEA|    null|        null| 60437|             Algeria|     null|    1995|    2021|          5|               4|                1|   1|
# |AGM00060461|  AG|    35.7|    -0.65|     22.0| null|           ORAN-PORT|    null|        null| 60461|             Algeria|     null|    1995|    2017|          4|               3|                1|   1|
# |AGM00060475|  AG|  35.432|    8.121|    811.1| null|CHEIKH LARBI TEBESSI|    null|        null| 60475|             Algeria|     null|    1958|    2021|          5|               4|                1|   1|
# |AGM00060514|  AG|  35.167|    2.317|    801.0| null|       KSAR CHELLALA|    null|        null| 60514|             Algeria|     null|    1995|    2021|          5|               4|                1|   1|
# |AGM00060515|  AG|  35.333|    4.206|    459.0| null|           BOU SAADA|    null|        null| 60515|             Algeria|     null|    1984|    2021|          4|               3|                1|   1|
# |AGM00060518|  AG|    35.3|    -1.35|     70.0| null|            BENI-SAF|    null|        null| 60518|             Algeria|     null|    1976|    2021|          4|               3|                1|   1|
# |AGM00060520|  AG|    35.2|   -0.617|    476.0| null|      SIDI-BEL-ABBES|    null|        null| 60520|             Algeria|     null|    1995|    2021|          5|               4|                1|   1|
# |AGM00060549|  AG|  33.536|   -0.242|   1175.0| null|            MECHERIA|    null|        null| 60549|             Algeria|     null|    1980|    2021|          5|               4|                1|   1|
# +-----------+----+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+---------+--------+--------+-----------+----------------+-----------------+----+
# only showing top 20 rows


####in case if you want to save it in csv.gz format
# stations_inventory.write.option("compression","gzip").csv('hdfs:///user/bpa78/outputs/ghcnd/Station.csv.gz')
# stations = spark.read.load('hdfs:///user/bpa78/outputs/ghcnd/Station.csv.gz', format = "com.databricks.spark.csv", header = True)


# In [23]: stations.show()
# +-----------+---+------+------+------+----+--------------------+----+----+-----+--------------------+----+----+----+----+----+----+----+
# |        _c0|_c1|   _c2|   _c3|   _c4| _c5|                 _c6| _c7| _c8|  _c9|                _c10|_c11|_c12|_c13|_c14|_c15|_c16|_c17|
# +-----------+---+------+------+------+----+--------------------+----+----+-----+--------------------+----+----+----+----+----+----+----+
# |AE000041196| AE|25.333|55.517|  34.0|null| SHARJAH INTER. AIRP| GSN|null|41196|United Arab Emirates|null|1944|2021|   4|   3|   1|   1|
# |AEM00041218| AE|24.262|55.609| 264.9|null|         AL AIN INTL|null|null|41218|United Arab Emirates|null|1994|2021|   4|   3|   1|   1|
# |AFM00040938| AF| 34.21|62.228| 977.2|null|               HERAT|null|null|40938|         Afghanistan|null|1973|2021|   5|   4|   1|   1|
# |AG000060611| AG| 28.05|9.6331| 561.0|null|           IN-AMENAS| GSN|null|60611|             Algeria|null|1958|2021|   5|   4|   1|   1|

###Size of these files to be checked using the code in hdfs_commands file line 283

daily_2022_1000 = daily_2022.limit(1000)
###Q3(f)
#Left Join daily_2022 1000 rows with stations_inventory
daily_stations = (
    daily_2022_1000
    .join(
        stations_inventory,
        on = 'ID',
        how = 'left'
        )
)    
#Stations that are in subset of daily table that are not in stations table at all?

not_in_stations_table = (
    daily_stations
        .select('ID', 'NAME')
        .where(F.col('NAME') == '')
)
not_in_stations_table.count()      
#Out[17]: 0


#Ref: https://stackoverflow.com/questions/47919022/how-to-compare-two-columns-in-two-different-dataframes-in-pyspark
not_in_stations = (  
    daily_stations
    .select('ID')
    .subtract(stations_inventory.select('ID'))
)
not_in_stations.count()
#Out[18]: 0

