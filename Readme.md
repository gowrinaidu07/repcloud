### Run the below commands in local
`gradle build`
## To run this needs gcloud authentication
https://github.com/googleapis/google-cloud-java#local-developmenttesting
https://developers.google.com/accounts/docs/application-default-credentials

When you run the below command it will create workflow template and run the pig job. check gradle mainClass

`gradle runWorkflow -DpigScript='gs://phw-code/gcp_uim_csv_hourly_reporting.pig' \
-DinputDir='gs://laas-raw/dateUTC=2022-08-05/hourUTC=01' \
-DoutputDir='gs://laas-hourly/dateUTC=2022-08-15/hourUTC=01'`
