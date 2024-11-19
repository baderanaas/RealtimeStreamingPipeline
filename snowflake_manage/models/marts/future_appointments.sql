SELECT * FROM {{ source('appointments_sources', 'all_appointments') }}
WHERE appointment_date >= CURRENT_DATE()