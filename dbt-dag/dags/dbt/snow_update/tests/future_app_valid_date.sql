SELECT * FROM {{ ref('future_appointments') }}
WHERE appointment_date >= CURRENT_DATE()