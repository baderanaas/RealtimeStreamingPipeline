SELECT *
FROM {{ source('get_all_appointments', 'ALL_APPOINTMENTS') }}
