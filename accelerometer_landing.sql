CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_landing(
timeStamp bigint,
user string,
x double,
y double,
z double 
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-human-ivleen/accelerometer/landing/'
