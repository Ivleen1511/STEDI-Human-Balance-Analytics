CREATE EXTERNAL TABLE IF NOT EXISTS step_trainer_landing(
sensorReadingTime bigint,
serialNumber string,
distanceFromObject int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-human-ivleen/step_trainer/landing/'
