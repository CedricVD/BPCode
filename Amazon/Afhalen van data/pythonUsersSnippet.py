return service.data().ga().get(
ids=table_id,
start_date='30daysAgo',
end_date='yesterday',
metrics='ga:sessionsPerUser',
dimensions='ga:Country,ga:ServiceProvider,ga:ClientID',
sort='-ga:sessionsPerUser',
start_index='1',
max_results='25').execute()