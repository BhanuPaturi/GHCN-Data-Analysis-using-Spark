#############Analysis#############

#start_pyspark_shell -e4 -c2 -w4 -m4
import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import * 
#### in case if you have using the data in csv.gz format then you have to define schema first
# schema_StationsInventory = StructType([
    # StructField("ID", StringType()),
    # StructField("CODE", StringType()),
    # StructField("LATITUDE", DoubleType()),
    # StructField("LONGITUDE", DoubleType()),
    # StructField("ELEVATION", DoubleType()),
    # StructField("STATE", StringType()),
    # StructField("NAME", StringType()),
    # StructField("GSN FLAG", StringType()),
    # StructField("HCN/CRN FLAG", StringType()),
    # StructField("WMO ID", StringType()),
    # StructField("COUNTRYNAME", StringType()),
    # StructField("STATENAME", StringType()),
    # StructField("MIN_YEAR", IntegerType()),
    # StructField("MAX_YEAR", IntegerType()),
    # StructField("NUM_ELEMENT", IntegerType()),
    # StructField("NUM_CORE_ELEMENT", IntegerType()),
    # StructField("NUM_OTHER_ELEMENT", IntegerType()),
    # StructField("PRCP", IntegerType()),
# ])
#stations_inventory = spark.read.load('hdfs:///user/bpa78/outputs/ghcnd/Stations', format = "com.databricks.spark.csv", header = "false", inferSchema = "false", schema = schema_StationsInventory)
stations_inventory = spark.read.load('hdfs:///user/bpa78/outputs/ghcnd/Stations', format = "com.databricks.spark.csv", header = True)

###Q1 (a)
#How many stations are there in total?
stations_inventory.select("ID").distinct().count()
#Out[20]: 118493


#How many stations were active in 2021?
active_stations_2021 = (
    stations_inventory
    .filter(F.col('MAX_YEAR') == 2021)
)
active_stations_2021.count()
#Out[6]: 38284


#How many stations in GCOS Surface Network(GSN)
GSN_stations = (
    stations_inventory
    .filter(F.col('GSN FLAG') == 'GSN')
)
GSN_stations.count()
#Out[7]: 991

#How many stations in US Historical Climatology Network(HCN)
HCN_stations = (
    stations_inventory
    .filter(F.col('HCN/CRN FLAG') == 'HCN')
)
HCN_stations.count()
#Out[8]: 1218

#How many stations in US Climate Reference Network(CRN)
CRN_stations = (
    stations_inventory
    .filter(F.col('HCN/CRN FLAG') == 'CRN')
)
CRN_stations.count()
#Out[9]: 0


#Stations in more than one of these network
more_networks = (
    stations_inventory
    .where((F.col('GSN FLAG') != '')& (F.col('HCN/CRN FLAG')!= ''))    
)
more_networks.count()
#Out[19]: 14

###Q1(b)
#Count the total number of stations in each country, and store the output in countries using the withColumnRenamed command
stations_per_country = (
    stations_inventory
    .groupBy(F.col('COUNTRYNAME'))
    .agg({'ID':'count'})
    .withColumnRenamed('count(ID)','NUM_STATIONS')
)
stations_per_country.show()
# :<EOF>
# +-----------------+------------+
# |      COUNTRYNAME|NUM_STATIONS|
# +-----------------+------------+
# |        Australia|       17088|
# |           Brunei|           1|
# |         Cameroon|           5|
# |            Ghana|          18|
# |        Indonesia|         104|
# |           Israel|          12|
# |    Cote D'Ivoire|          21|
# |        Nicaragua|           6|
# |     Burkina Faso|          13|
# |Equatorial Guinea|           2|
# |             Togo|          10|
# |      Afghanistan|           4|
# |          Armenia|          53|
# | Congo (Kinshasa)|          13|
# |          Croatia|          14|
# |             Laos|          15|
# |          Romania|          30|
# |           Sweden|        1721|
# |         Thailand|          54|
# |           Turkey|          59|
# +-----------------+------------+
# only showing top 20 rows
#Out[31]: 219
stations_per_country.write.csv('hdfs:///user/bpa78/outputs/ghcnd/countries', mode = 'overwrite', header = True)

#countries = spark.read.load('hdfs:///user/bpa78/outputs/ghcnd/countries', format = "com.databricks.spark.csv", header = True)

stations_per_state = (
    stations_inventory
    .groupBy(F.col('STATE'))
    .agg({'ID':'count'})
    .withColumnRenamed('count(ID)','NUM_STATIONS')
)
stations_per_state.show()

#Out[48]: 34

stations_per_state.write.csv('hdfs:///user/bpa78/outputs/ghcnd/states', mode = 'overwrite', header = True)
#states = spark.read.load('hdfs:///user/bpa78/outputs/ghcnd/states', format = "com.databricks.spark.csv", header = True)

#how many stations are there in Northern Hemisphere only?
stations_NH = (
    stations_inventory
    .filter(F.col('LATITUDE') > 0)
)
stations_NH.count()
#Out[39]: 93156

# stations_US_territories = (
    # stations_inventory
    # .filter((F.col('COUNTRYNAME').contains("United States")) & (F.col('CODE') != 'US'))
    # .groupBy(F.col('COUNTRYNAME'))
    # .count()    
# )

# stations_US_territories.show(10,False)
# :<EOF>
# +----------------------------------------+-----+
# |COUNTRYNAME                             |count|
# +----------------------------------------+-----+
# |Northern Mariana Islands [United States]|11   |
# |Midway Islands [United States}          |2    |
# |Guam [United States]                    |21   |
# |American Samoa [United States]          |21   |
# |Puerto Rico [United States]             |222  |
# |Virgin Islands [United States]          |54   |
# |Wake Island [United States]             |1    |
# |Johnston Atoll [United States]          |4    |
# |Palmyra Atoll [United States]           |3    |
# +----------------------------------------+-----+


#how many stations are there in total in territories of US excluding US?
stations_US_territories = (
    stations_inventory
    .filter((F.col('COUNTRYNAME').contains("United States")) & (F.col('CODE') != 'US'))    
)
stations_US_territories.count()

#Out[77]: 339

####Q2 (a)

#User defined function to compute distance between two stations


#Ref:https://www.igismap.com/haversine-formula-calculate-geographic-distance-earth/
from math import radians, sin, cos, atan2, sqrt

def getDistance(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    R = 6373.0 #radius of the earth in kms
    diff_lon = lon2 - lon1
    diff_lat = lat2 - lat1
    
    area = (sin(diff_lat / 2))**2 + cos(lat1) * cos(lat2) * (sin(diff_lon / 2)) ** 2
    centralAngle = 2 * atan2(sqrt(area), sqrt(1 - area))
    
    distance = centralAngle * R
    return round(distance, 2)
    
### To convert to UDF:
udf_getDistance = F.udf(getDistance)

####Q2 (b)
stations_NZ = (
    stations_inventory
    .filter(F.col('CODE') == 'NZ')
)

station_NZ1 = stations_NZ.select('ID', 'NAME', 'LATITUDE', 'LONGITUDE').toDF('ID1', 'NAME1', 'LATITUDE1', 'LONGITUDE1')
station_NZ2 = stations_NZ.select('ID', 'NAME', 'LATITUDE', 'LONGITUDE').toDF('ID2', 'NAME2', 'LATITUDE2', 'LONGITUDE2')

##Cross join station_NZ1 to station_NZ2
stations_NZ_pair = (
    station_NZ1.crossJoin(station_NZ2)
    .where(F.col('ID1') < F.col('ID2'))
)

stations_NZ_pair.show()
# :<EOF>
# +-----------+----------------+---------+----------+-----------+-------------------+---------+----------+
# |        ID1|           NAME1|LATITUDE1|LONGITUDE1|        ID2|              NAME2|LATITUDE2|LONGITUDE2|
# +-----------+----------------+---------+----------+-----------+-------------------+---------+----------+
# |NZ000093417| PARAPARAUMU AWS|    -40.9|   174.983|NZ000933090|   NEW PLYMOUTH AWS|  -39.017|   174.183|
# |NZ000093417| PARAPARAUMU AWS|    -40.9|   174.983|NZM00093781|  CHRISTCHURCH INTL|  -43.489|   172.532|
# |NZ000093417| PARAPARAUMU AWS|    -40.9|   174.983|NZ000936150| HOKITIKA AERODROME|  -42.717|   170.983|
# |NZ000093417| PARAPARAUMU AWS|    -40.9|   174.983|NZ000939450|CAMPBELL ISLAND AWS|   -52.55|   169.167|
# |NZ000093417| PARAPARAUMU AWS|    -40.9|   174.983|NZM00093678|           KAIKOURA|  -42.417|     173.7|
# |NZ000093417| PARAPARAUMU AWS|    -40.9|   174.983|NZM00093929| ENDERBY ISLAND AWS|  -50.483|     166.3|
# |NZ000093417| PARAPARAUMU AWS|    -40.9|   174.983|NZ000093844|INVERCARGILL AIRPOR|  -46.417|   168.333|
# |NZ000093417| PARAPARAUMU AWS|    -40.9|   174.983|NZ000093994| RAOUL ISL/KERMADEC|   -29.25|  -177.917|
# |NZ000093417| PARAPARAUMU AWS|    -40.9|   174.983|NZ000937470|         TARA HILLS|  -44.517|     169.9|
# |NZ000093417| PARAPARAUMU AWS|    -40.9|   174.983|NZ000939870|CHATHAM ISLANDS AWS|   -43.95|  -176.567|
# |NZ000093417| PARAPARAUMU AWS|    -40.9|   174.983|NZM00093439|WELLINGTON AERO AWS|  -41.333|     174.8|
# |NZ000093417| PARAPARAUMU AWS|    -40.9|   174.983|NZM00093110|  AUCKLAND AERO AWS|    -37.0|     174.8|
# |NZ000933090|NEW PLYMOUTH AWS|  -39.017|   174.183|NZM00093781|  CHRISTCHURCH INTL|  -43.489|   172.532|
# |NZ000933090|NEW PLYMOUTH AWS|  -39.017|   174.183|NZ000936150| HOKITIKA AERODROME|  -42.717|   170.983|
# |NZ000933090|NEW PLYMOUTH AWS|  -39.017|   174.183|NZ000939450|CAMPBELL ISLAND AWS|   -52.55|   169.167|
# |NZ000933090|NEW PLYMOUTH AWS|  -39.017|   174.183|NZM00093678|           KAIKOURA|  -42.417|     173.7|
# |NZ000933090|NEW PLYMOUTH AWS|  -39.017|   174.183|NZM00093929| ENDERBY ISLAND AWS|  -50.483|     166.3|
# |NZ000933090|NEW PLYMOUTH AWS|  -39.017|   174.183|NZ000937470|         TARA HILLS|  -44.517|     169.9|
# |NZ000933090|NEW PLYMOUTH AWS|  -39.017|   174.183|NZ000939870|CHATHAM ISLANDS AWS|   -43.95|  -176.567|
# |NZ000933090|NEW PLYMOUTH AWS|  -39.017|   174.183|NZM00093439|WELLINGTON AERO AWS|  -41.333|     174.8|
# +-----------+----------------+---------+----------+-----------+-------------------+---------+----------+
# only showing top 20 rows


##Creating a new column for distance
distance_NZstations = (
    stations_NZ_pair
    .withColumn('DISTANCE', udf_getDistance(F.col('LONGITUDE1'), F.col('LATITUDE1'), F.col('LONGITUDE2'), F.col('LATITUDE2')))
)

distance_NZstations = (
    distance_NZstations
    .withColumn('DISTANCE', F.col('DISTANCE').cast(DoubleType()))    
)

distance_NZstations.write.csv('hdfs:///user/bpa78/outputs/ghcnd/distance_NZstations', mode='overwrite', header=True)

distance_NZstations.orderBy(F.col('DISTANCE').asc()).show()
# In [18]: distance_NZstations.orderBy(F.col('DISTANCE').asc()).show()
# +-----------+-------------------+---------+----------+-----------+-------------------+---------+----------+--------+
# |        ID1|              NAME1|LATITUDE1|LONGITUDE1|        ID2|              NAME2|LATITUDE2|LONGITUDE2|DISTANCE|
# +-----------+-------------------+---------+----------+-----------+-------------------+---------+----------+--------+
# |NZ000093417|    PARAPARAUMU AWS|    -40.9|   174.983|NZM00093439|WELLINGTON AERO AWS|  -41.333|     174.8|   50.54|
# |NZM00093439|WELLINGTON AERO AWS|  -41.333|     174.8|NZM00093678|           KAIKOURA|  -42.417|     173.7|  151.12|
# |NZ000936150| HOKITIKA AERODROME|  -42.717|   170.983|NZM00093781|  CHRISTCHURCH INTL|  -43.489|   172.532|  152.31|
# |NZM00093678|           KAIKOURA|  -42.417|     173.7|NZM00093781|  CHRISTCHURCH INTL|  -43.489|   172.532|  152.51|
# |NZ000093417|    PARAPARAUMU AWS|    -40.9|   174.983|NZM00093678|           KAIKOURA|  -42.417|     173.7|  199.59|
# |NZ000936150| HOKITIKA AERODROME|  -42.717|   170.983|NZ000937470|         TARA HILLS|  -44.517|     169.9|  218.38|
# |NZ000093417|    PARAPARAUMU AWS|    -40.9|   174.983|NZ000933090|   NEW PLYMOUTH AWS|  -39.017|   174.183|  220.27|
# |NZ000936150| HOKITIKA AERODROME|  -42.717|   170.983|NZM00093678|           KAIKOURA|  -42.417|     173.7|  225.05|
# |NZ000933090|   NEW PLYMOUTH AWS|  -39.017|   174.183|NZM00093110|  AUCKLAND AERO AWS|    -37.0|     174.8|  230.77|
# |NZ000937470|         TARA HILLS|  -44.517|     169.9|NZM00093781|  CHRISTCHURCH INTL|  -43.489|   172.532|  239.61|
# |NZ000093844|INVERCARGILL AIRPOR|  -46.417|   168.333|NZ000937470|         TARA HILLS|  -44.517|     169.9|  244.13|
# |NZ000093012|            KAITAIA|    -35.1|   173.267|NZM00093110|  AUCKLAND AERO AWS|    -37.0|     174.8|  252.32|
# |NZ000933090|   NEW PLYMOUTH AWS|  -39.017|   174.183|NZM00093439|WELLINGTON AERO AWS|  -41.333|     174.8|  262.89|
# |NZM00093439|WELLINGTON AERO AWS|  -41.333|     174.8|NZM00093781|  CHRISTCHURCH INTL|  -43.489|   172.532|  303.62|
# |NZ000939450|CAMPBELL ISLAND AWS|   -52.55|   169.167|NZM00093929| ENDERBY ISLAND AWS|  -50.483|     166.3|  303.66|
# |NZ000093292| GISBORNE AERODROME|   -38.65|   177.983|NZ000933090|   NEW PLYMOUTH AWS|  -39.017|   174.183|  331.75|
# |NZ000093292| GISBORNE AERODROME|   -38.65|   177.983|NZM00093110|  AUCKLAND AERO AWS|    -37.0|     174.8|  334.47|
# |NZ000936150| HOKITIKA AERODROME|  -42.717|   170.983|NZM00093439|WELLINGTON AERO AWS|  -41.333|     174.8|  350.91|
# |NZ000093417|    PARAPARAUMU AWS|    -40.9|   174.983|NZM00093781|  CHRISTCHURCH INTL|  -43.489|   172.532|  351.71|
# |NZ000093292| GISBORNE AERODROME|   -38.65|   177.983|NZ000093417|    PARAPARAUMU AWS|    -40.9|   174.983|  358.29|
# +-----------+-------------------+---------+----------+-----------+-------------------+---------+----------+--------+
# only showing top 20 rows

distance_NZstations.count()
#Out[49]: 105

#####Q3 Analysis#############
###(a) Please check hdfs commands in hdfs.py file

###(b)
#defining the schema for daily
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

#Load and count the number of observations in 2021 for daily
daily_2021 = spark.read.load("hdfs:///data/ghcnd/daily/2021.csv.gz", format = "csv", header = "false", inferSchema = "false", schema = schema_Daily)
daily_2021.count()
#Out[4]: 34657282

#Load and count the number of observations in 2022 for daily
daily_2022 = spark.read.load("hdfs:///data/ghcnd/daily/2022.csv.gz", format = "csv", header = "false", inferSchema = "false", schema = schema_Daily)
daily_2022.count()
#Out[5]: 5971307

#(c)
#Load and count the number of observations from 2014 to 2022 for daily
daily_2014_2022 = spark.read.load("hdfs:///data/ghcnd/daily/20{14,15,16,17,18,19,20,21,22}.csv.gz", format = "csv", header = "false", inferSchema = "false", schema = schema_Daily)
daily_2014_2022.count()
#Out[6]: 284918108



#####Q4 Analysis#############
#(a)
#Load and count the number of observations for all the years for daily
daily_all = spark.read.load("hdfs:///data/ghcnd/daily/*.csv.gz", format = "csv", header = "false", inferSchema = "false", schema = schema_Daily)
daily_all.count()
#Out[7]: 3000243596

#(b)
#Filter the 5 core elements?
daily_5_elements = (
    daily_all
    .filter(F.col('ELEMENT').isin('PRCP', 'SNOW', 'SNWD', 'TMAX', 'TMIN'))
    .groupby('ELEMENT')
    .agg({'ID':'count'})
    .withColumnRenamed('count(ID)','COUNT')
)
daily_5_elements.show()

# +-------+----------+
# |ELEMENT|     COUNT|
# +-------+----------+
# |   SNOW| 341985067|
# |   SNWD| 289981374|
# |   PRCP|1043785667|
# |   TMIN| 444271327|
# |   TMAX| 445623712|
# +-------+----------+

#(c)


TMIN_TMAX = (
    daily_all
    .select('ID', 'ELEMENT', 'DATE')
    .filter(F.col('ELEMENT').isin('TMAX','TMIN'))
    .groupBy('ID','DATE')
    .pivot('ELEMENT')
    .agg({'ELEMENT':'count'})
)
# In [6]: TMIN_TMAX.count()
# Out[6]: 454432517

#how many observations of TMIN do not have a corresponding observation of TMAX?
without_TMAX = (
    TMIN_TMAX
    .filter((F.col('TMIN').isNotNull()) & (F.col('TMAX').isNull()))   
)
without_TMAX.count()
#Out[7]: 8808805


#how many different stations contributed to these observation?
stations_TMIN_noTMAX = (
    without_TMAX
    .select(F.countDistinct('ID'))
)
stations_TMIN_noTMAX.show()

# +------------------+
# |count(DISTINCT ID)|
# +------------------+
# |             27650|
# +------------------+


#(d)
#To obtain all observations of TMIN and TMAX for all stations in New Zealand


TMIN_TMAX_NZ = (
    daily_all
    .select('ID', 'ELEMENT', 'DATE', 'VALUE')
    .filter((F.col('ELEMENT').isin('TMAX','TMIN'))& (F.col('ID').substr(1,2) == 'NZ'))       
)

TMIN_TMAX_NZ = (
    TMIN_TMAX_NZ
    .join(
        stations_inventory
        .select('ID', 'NAME'),
        on = 'ID',
        how = 'left'
        )
)
TMIN_TMAX_NZ_year = (
    TMIN_TMAX_NZ
    .withColumn('YEAR', F.trim(F.col('DATE').substr(1,4)))
    .select('ID','NAME','ELEMENT','YEAR','VALUE')
)
TMIN_TMAX_NZ_year.show()

# :<EOF>
# +-----------+-------------------+-------+----+-----+
# |         ID|               NAME|ELEMENT|YEAR|VALUE|
# +-----------+-------------------+-------+----+-----+
# |NZ000936150| HOKITIKA AERODROME|   TMAX|2010|324.0|
# |NZM00093110|  AUCKLAND AERO AWS|   TMAX|2010|215.0|
# |NZM00093110|  AUCKLAND AERO AWS|   TMIN|2010|153.0|
# |NZM00093678|           KAIKOURA|   TMAX|2010|242.0|
# |NZM00093678|           KAIKOURA|   TMIN|2010| 94.0|
# |NZ000093292| GISBORNE AERODROME|   TMAX|2010|297.0|
# |NZ000093292| GISBORNE AERODROME|   TMIN|2010| 74.0|
# |NZM00093781|  CHRISTCHURCH INTL|   TMAX|2010|324.0|
# |NZM00093439|WELLINGTON AERO AWS|   TMAX|2010|204.0|
# |NZM00093439|WELLINGTON AERO AWS|   TMIN|2010|134.0|
# |NZ000093844|INVERCARGILL AIRPOR|   TMAX|2010|232.0|
# |NZ000093844|INVERCARGILL AIRPOR|   TMIN|2010| 96.0|
# |NZ000093417|    PARAPARAUMU AWS|   TMAX|2010|180.0|
# |NZ000093417|    PARAPARAUMU AWS|   TMIN|2010|125.0|
# |NZ000933090|   NEW PLYMOUTH AWS|   TMAX|2010|197.0|
# |NZ000933090|   NEW PLYMOUTH AWS|   TMIN|2010| 82.0|
# |NZM00093110|  AUCKLAND AERO AWS|   TMAX|2010|241.0|
# |NZM00093110|  AUCKLAND AERO AWS|   TMIN|2010|153.0|
# |NZM00093678|           KAIKOURA|   TMAX|2010|289.0|
# |NZ000093292| GISBORNE AERODROME|   TMAX|2010|302.0|
# +-----------+-------------------+-------+----+-----+
# only showing top 20 rows--------+


#how many observations are there?
TMIN_TMAX_NZ.count()
#Out[28]: 472271

#how many years are covered by the observations

TMIN_TMAX_NZ_year.select('YEAR').distinct().count()
#Out[8]: 83

#Save the result to output directory
TMIN_TMAX_NZ_year.write.csv('hdfs:///user/bpa78/outputs/ghcnd/TMIN_TMAX_NZ', header = True, mode = 'overwrite') 

#To copy the TMIN_TMAX_NZ.csv from HDFS to local home directory please check hdfs.py
#To count number of rows please check hdfs.py



#(e) 
#Filter the percipitation observations by year and country

perc_year_country =  (
    daily_all
    .select('ID', 'DATE', 'ELEMENT', 'VALUE')
    .filter(F.col('ELEMENT') == 'PRCP')
    .withColumn('COUNTRY_CODE', F.trim(F.col('ID').substr(1,2)))
    .withColumn('YEAR', F.trim(F.col('DATE').substr(1,4)))
)

#Group the percipitation observations and compute the average rainfall in each year for each country
    
avg_rainfall_year_country = (
    perc_year_country
    .groupby('COUNTRY_CODE', 'YEAR')
    .agg({'VALUE':'mean'})
    .withColumnRenamed('avg(VALUE)', 'AVG_RAINFALL')
    .orderBy('AVG_RAINFALL', ascending = False)
)

avg_rainfall_year_country.show()

# +------------+----+------------------+
# |COUNTRY_CODE|YEAR|      AVG_RAINFALL|
# +------------+----+------------------+
# |          EK|2000|            4361.0|
# |          DR|1975|            3414.0|
# |          LA|1974|            2480.5|
# |          BH|1978| 2244.714285714286|
# |          NN|1979|            1967.0|
# |          CS|1974|            1820.0|
# |          BH|1979|1755.5454545454545|
# |          NS|1973|            1710.0|
# |          UC|1978|1675.0384615384614|
# |          BH|1977|1541.7142857142858|
# |          HO|1978|1469.6122448979593|
# |          UC|1977|1442.5384615384614|
# |          NN|1978|1292.8695652173913|
# |          HO|1977| 1284.138888888889|
# |          TD|1978|            1265.0|
# |          GY|1976|1213.3333333333333|
# |          UC|1979|            1168.2|
# |          TS|1973|            1162.0|
# |          BM|2006|            1152.0|
# |          EK|2001|            1100.0|
# +------------+----+------------------+
# only showing top 20 rows

##EK equator Guinea has the highest average rainfall 

#to save the result in output directory
avg_rainfall_year_country.write.csv('hdfs:///user/bpa78/outputs/ghcnd/avgRainfallCountry', header = True, mode = 'overwrite')

