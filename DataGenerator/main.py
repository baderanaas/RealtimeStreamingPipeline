import json
import time
import random

from faker import Faker
from confluent_kafka import SerializingProducer
from datetime import datetime

fake = Faker()


def generate_healthcare_data():
    patient = fake.simple_profile()
    symptoms = [
        "Fever", "Cough", "Headache", "Fatigue", "Nausea", 
        "Sore throat", "Chest pain", "Shortness of breath"
    ]
    diagnoses = [
        "Common Cold", "Flu", "COVID-19", "Hypertension", 
        "Diabetes", "Anemia", "Migraine", "Asthma"
    ]
    treatments = [
        "Rest and hydration", "Medication", "Therapy", "Surgery", 
        "Lifestyle changes", "Physical therapy"
    ]

    return {
        "appointmentId": fake.uuid4(),
        "patientId": patient["username"],
        "patientName": patient["name"],
        "gender": patient["sex"],
        "age": random.randint(1, 100),
        "address": fake.address(),
        "symptoms": random.sample(symptoms, random.randint(1, 3)),
        "diagnosis": random.choice(diagnoses),
        "treatmentPlan": random.choice(treatments),
        "appointmentDate": datetime.utcnow().strftime(
            "%Y-%m-%dT%H:%M:%S.%f%z"
        ),
        "doctorName": fake.name(),
        "hospital": fake.company(),
        "insuranceProvider": fake.company(),
        "paymentMethod": random.choice(["insurance", "self-pay"]),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Message delivered to {msg.topic}: {msg.partition()}")


def main():
    topic = "healthcare_appointments"
    producer = SerializingProducer({"bootstrap.servers": "localhost:9092"})

    curr_time = datetime.now()

    while (datetime.now() - curr_time).seconds < 180:
        try:
            appointment = generate_healthcare_data()
            producer.produce(
                topic,
                key=appointment["appointmentId"],
                value=json.dumps(appointment),
                on_delivery=delivery_report,
            )
            print(appointment)
            producer.poll(0)
            time.sleep(5)
        except BufferError:
            print("Buffer full! Waiting...")
            time.sleep(1)
        except Exception as e:
            print(e)


if __name__ == "__main__":
    main()
