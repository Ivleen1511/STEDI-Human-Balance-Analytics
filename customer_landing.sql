CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing(
serialnumber string,
sharewithpublicasofdate string,
birthday string,
registrationdate string,
sharewithresearchasofdate string,
customername string,
email string,
lastupdatedate string,
phone string,
sharewithfriendsasofdate string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-human-ivleen/customer/landing/'



