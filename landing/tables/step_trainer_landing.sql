CREATE EXTERNAL TABLE `step_trainer_landing`(
  `sensorreadingtime` bigint COMMENT 'from deserializer',
  `serialnumber` string COMMENT 'from deserializer',
  `distancefromobject` int COMMENT 'from deserializer')
COMMENT 'Landing zone for step trainer data'
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'case.insensitive'='TRUE',
  'dots.in.keys'='FALSE',
  'ignore.malformed.json'='FALSE',
  'mapping'='TRUE')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://daifs-nanodegree-bucket/step_trainer/landing'
TBLPROPERTIES (
  'classification'='json',
  'transient_lastDdlTime'='1699795283',
  'write.compression'='NONE')
  