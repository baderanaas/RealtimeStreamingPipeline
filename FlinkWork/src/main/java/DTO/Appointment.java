package DTO;

import lombok.Data;

import java.sql.Timestamp;
import java.util.List;

@Data
public class Appointment {
    private String appointmentId;
    private String patientId;
    private String patientName;
    private String gender;
    private int age;
    private String address;
    private List<String> symptoms;
    private String diagnosis;
    private String treatmentPlan;
    private Timestamp appointmentDate;
    private String doctorName;
    private String hospital;
    private String insuranceProvider;
    private String paymentMethod;
}
