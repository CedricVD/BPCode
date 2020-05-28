set userprofile=C:\Users\%username%
set AWS_CONFIG_FILE=C:\cvand\..\.aws\config
set AWS_ACCESS_KEY_ID=<ID>
set AWS_SECRET_ACCESS_KEY=<KEY>
aws s3 sync "C:\<pathToFolder>" s3://<awsbucketurl>