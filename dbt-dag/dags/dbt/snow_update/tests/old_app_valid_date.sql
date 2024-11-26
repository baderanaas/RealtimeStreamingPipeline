SELECT * FROM {{ ref('passed_appointments') }}
WHERE appointment_date <= CURRENT_DATE()