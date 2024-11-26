SELECT * FROM {{ ref('all_appointments') }}
WHERE appointment_date < CURRENT_DATE()