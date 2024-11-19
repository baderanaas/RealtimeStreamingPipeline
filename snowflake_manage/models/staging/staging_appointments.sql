SELECT *
FROM {{ source('appointments_sources', 'all_appointments') }}