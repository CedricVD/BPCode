CREATE TEMP FUNCTION URLDECODE(url STRING) AS ((
SELECT STRING_AGG(
IF(REGEXP_CONTAINS(y, r'^%[0-9a-fA-F]{2}'), 
SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX(REPLACE(y, '%', ''))), y), '' 
ORDER BY i
)
FROM UNNEST(REGEXP_EXTRACT_ALL(url, 
    r"%[0-9a-fA-F]{2}(?:%[0-9a-fA-F]{2})*|[^%]+")) y
WITH OFFSET AS i 
));

SELECT 
TIMESTAMP_MILLIS(cast(hit_timestamp as int64)) utc_timestamp,
date(TIMESTAMP_MILLIS(cast(hit_timestamp as int64))) utc_date,
hit_timestamp as hit_unix_milliseconds,
REGEXP_EXTRACT(payload, "uid=([^&]+)") as user_id, 
REGEXP_EXTRACT(payload, "cid=([^&]+)") as client_id,
REGEXP_EXTRACT(payload, "t=([^&]+)") as hit_type, 
URLDECODE(REGEXP_EXTRACT(payload, "ec=([^&]+)")) as event_category,
URLDECODE(REGEXP_EXTRACT(payload, "ea=([^&]+)")) as event_action,
URLDECODE(REGEXP_EXTRACT(payload, "el=([^&]+)")) as event_label,
REGEXP_EXTRACT(payload, "ev=([^&]+)") as event_value, 
URLDECODE(REGEXP_EXTRACT(payload, "cd1=([^&]+)")) as custom_dimension_1,
URLDECODE(REGEXP_EXTRACT(payload, "cd2=([^&]+)")) as custom_dimension_2,
URLDECODE(REGEXP_EXTRACT(payload, "cg1=([^&]+)")) as content_grouping_1,
URLDECODE(REGEXP_EXTRACT(payload, "cg2=([^&]+)")) as content_grouping_2,
URLDECODE(REGEXP_EXTRACT(payload, "dh=([^&]+)")) as hostname,
URLDECODE(REGEXP_EXTRACT(payload, "dl=([^&]+)")) as page_url,
URLDECODE(REGEXP_EXTRACT(payload, "dp=([^&]+)")) as page_path,
URLDECODE(REGEXP_EXTRACT(payload, "dr=([^&]+)")) as referrer_source,
URLDECODE(REGEXP_EXTRACT(payload, "dt=([^&]+)")) as page_title, 
REGEXP_EXTRACT(payload, "gclid=([^&]+)") as google_ads_id,
REGEXP_EXTRACT(payload, "dclid=([^&]+)") as display_ads_id,     
REGEXP_EXTRACT(payload, "ni=([^&]+)") as non_interaction_hit,
URLDECODE(REGEXP_EXTRACT(payload, "ua=([^&]+)")) as user_agent,  
URLDECODE(REGEXP_EXTRACT(payload, "de=([^&]+)")) as document_encoding, 
URLDECODE(REGEXP_EXTRACT(payload, "ul=([^&]+)")) as user_language,  
REGEXP_EXTRACT(payload, "_v=([^&]+)") as protocol_version,
REGEXP_EXTRACT(payload, "je=([^&]+)") as java_enable,
REGEXP_EXTRACT(payload, "sd=([^&]+)") as screen_color,
REGEXP_EXTRACT(payload, "sr=([^&]+)") as screen_resolution,
URLDECODE(REGEXP_EXTRACT(payload, "tid=([^&]+)")) as ga_property_id,
REGEXP_EXTRACT(payload, "vp=([^&]+)") as view_port,
REGEXP_EXTRACT(payload, "_gid=([^&]+)") as _gid,
REGEXP_EXTRACT(payload, "aip=([^&]+)") as anonymized_ip, 
REGEXP_EXTRACT(payload, "ds=([^&]+)") as data_source,  
REGEXP_EXTRACT(payload, "sc=([^&]+)") as session_controller,   
REGEXP_EXTRACT(payload, "uip=([^&]+)") as ip_address, 
REGEXP_EXTRACT(payload, "gtm=([^&]+)") as gtm_id
FROM `bachelorproef-275417.GARawData.RawData`  