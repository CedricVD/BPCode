return service.data().ga().get(
    ids=table_id,
    start_date='30daysAgo',
    end_date='yesterday',
    metrics='ga:pageviews',
    dimensions='ga:Country,ga:SessionID',
    sort='-ga:pageviews',
    start_index='1',
    max_results='25')