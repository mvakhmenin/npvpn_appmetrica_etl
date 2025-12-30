app_ids = {
    'NoProblem VPN': 4804657,
    'ProxyMasterDev': 4794559
}

appmetrica_endpoints = {
    'installations': '/logs/v1/export/installations.json',
    'events': '/logs/v1/export/events.json'
}

appmetrica_fields = {
    'installations': 'application_id,installation_id,attributed_touch_type,click_datetime,click_id,click_ipv6,click_timestamp,click_url_parameters,click_user_agent,profile_id,publisher_id,publisher_name,tracker_name,tracking_id,install_datetime,install_ipv6,install_receive_datetime,install_receive_timestamp,install_timestamp,is_reattribution,is_reinstallation,match_type,appmetrica_device_id,city,connection_type,country_iso_code,device_locale,device_manufacturer,device_model,device_type,google_aid,oaid,ios_ifa,ios_ifv,mcc,mnc,operator_name,os_name,os_version,windows_aid,app_package_name,app_version_name',
    'events': 'event_datetime,event_json,event_name,event_receive_datetime,event_receive_timestamp,event_timestamp,session_id,installation_id,appmetrica_device_id,city,connection_type,country_iso_code,device_ipv6,device_locale,device_manufacturer,device_model,device_type,google_aid,ios_ifa,ios_ifv,mcc,mnc,operator_name,original_device_model,os_name,os_version,profile_id,windows_aid,app_build_number,app_package_name,app_version_name,application_id'
}

appmetrica_ch_tables = {
    'installations':{
        'table_name':'noproblem.appmetrica_installations',
        'date_time_field':'install_datetime'
        },
    'events': {
        'table_name':'noproblem.appmetrica_events',
        'date_time_field':'event_datetime'
        }
}