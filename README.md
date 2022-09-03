# GHCN-Data-Analysis-using-Spark
PySpark, HDFS


Here we study some of the weather data contained in the Global Historical Climate
Network (GHCN), an integrated database of climate summaries from land surface stations around the
world. The data covers the last 175 years and is collected from more than 20 independent sources,
each of which have been subjected to quality assurance reviews.

• Global Historical Climatology Network (GHCN)

• GHCN Daily
The daily climate summaries contain records from over 100,000 stations in 180 countries and territories
around the world. There are several daily variables, including maximum and minimum temperature, total
daily precipitation, snowfall, and snow depth; however, about half of the stations report precipitation only.
The records vary by station and cover intervals ranging from less than a year to 175 years in total.
The daily climate summaries are supplemented by metadata further identifying the stations, countries,
states, and elements inventory specific to each station and time period. These provide human readable
names, geographical coordinates, elevations, and date ranges for each station variable in the inventory.

Daily

The daily climate summaries are comma separated, where each field is separated by a comma ( , ) and
where null fields are empty. A single row of data contains an observation for a specific station and day,
and each variable collected by the station is on a separate row.
The following information defines each field in a single row of data covering one station day. Each field
described below is separated by a comma ( , ) and follows the order below from left to right in each row.

```
Name              Type        Summary

ID                Character   Station code
DATE              Date        Observation date formatted as YYYYMMDD
ELEMENT           Character   Element type indicator
VALUE             Real        Data value for ELEMENT
MEASUREMENT FLAG  Character   Measurement Flag
QUALITY FLAG      Character   Quality Flag
SOURCE FLAG       Character   Source Flag
OBSERVATION       TIME        Time Observation time formatted as HHMM

The specific ELEMENT codes and their units are explained in Section III of the GHCN Daily README,
along with the MEASUREMENT FLAG, QUALITY FLAG, and SOURCE FLAG. The OBSERVATION
TIME field is populated using the NOAA / NCDC Multinetwork Metadata System (MMS).
```

Metadata

The station, country, state, and variable inventory metadata files are fixed width text formatted, where
each column has a fixed width specified by a character range and where null fields are represented by
whitespace instead.
