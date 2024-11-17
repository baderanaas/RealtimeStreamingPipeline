package FlinkWork;

import DTO.Appointment;
import deserializer.JSONValueDeserializationSchema;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcSink;


public class DataStreamJob {
	private static final String jdbcUrl = "jdbc:postgresql://localhost:6543/appointments";
	private static final String username = "postgres";
	private static final String password = "admin";

	public static void main(String[] args) throws Exception {
		System.setProperty("log4j2.debug", "true");
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String topic = "healthcare_appointments";

		KafkaSource<Appointment> source = KafkaSource.<Appointment>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics(topic)
				.setGroupId("pipeline-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new JSONValueDeserializationSchema())
				.build();

		DataStream<Appointment> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

		stream.print();

		JdbcExecutionOptions execOptions = new JdbcExecutionOptions.Builder()
				.withBatchSize(1000)
				.withBatchIntervalMs(200)
				.withMaxRetries(5)
				.build();

		JdbcConnectionOptions connOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
				.withUrl(jdbcUrl)
				.withDriverName("org.postgresql.Driver")
				.withUsername(username)
				.withPassword(password)
				.build();

		stream.addSink(JdbcSink.sink(
				"CREATE TABLE IF NOT EXISTS appointments (" +
						"appointment_id VARCHAR(255) PRIMARY KEY, " +
						"patient_id VARCHAR(255), " +
						"patient_name VARCHAR(255), " +
						"gender VARCHAR(50), " +
						"age INTEGER, " +
						"address TEXT, " +
						"symptoms TEXT[], " +
						"diagnosis TEXT, " +
						"treatment_plan TEXT, " +
						"appointment_date TIMESTAMP, " +
						"doctor_name VARCHAR(255), " +
						"hospital VARCHAR(255), " +
						"insurance_provider VARCHAR(255), " +
						"payment_method VARCHAR(255)" +
						")",
				(JdbcStatementBuilder<Appointment>) (preparedStatement, appointment) -> {

				},
				execOptions,
				connOptions
		)).name("Create Appointments Table Sink");

		stream.addSink(JdbcSink.sink(
				"INSERT INTO appointments (appointment_id, patient_id, patient_name, gender, age, address, symptoms, " +
						"diagnosis, treatment_plan, appointment_date, doctor_name, hospital, insurance_provider, payment_method) " +
						"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
						"ON CONFLICT (appointment_id) DO UPDATE SET " +
						"patient_id = EXCLUDED.patient_id, " +
						"patient_name = EXCLUDED.patient_name, " +
						"gender = EXCLUDED.gender, " +
						"age = EXCLUDED.age, " +
						"address = EXCLUDED.address, " +
						"symptoms = EXCLUDED.symptoms, " +
						"diagnosis = EXCLUDED.diagnosis, " +
						"treatment_plan = EXCLUDED.treatment_plan, " +
						"appointment_date = EXCLUDED.appointment_date, " +
						"doctor_name = EXCLUDED.doctor_name, " +
						"hospital = EXCLUDED.hospital, " +
						"insurance_provider = EXCLUDED.insurance_provider, " +
						"payment_method = EXCLUDED.payment_method",
				(JdbcStatementBuilder<Appointment>) (preparedStatement, appointment) -> {
					preparedStatement.setString(1, appointment.getAppointmentId());
					preparedStatement.setString(2, appointment.getPatientId());
					preparedStatement.setString(3, appointment.getPatientName());
					preparedStatement.setString(4, appointment.getGender());
					preparedStatement.setInt(5, appointment.getAge());
					preparedStatement.setString(6, appointment.getAddress());
					preparedStatement.setArray(7, preparedStatement.getConnection()
							.createArrayOf("TEXT", appointment.getSymptoms().toArray()));
					preparedStatement.setString(8, appointment.getDiagnosis());
					preparedStatement.setString(9, appointment.getTreatmentPlan());
					preparedStatement.setTimestamp(10, appointment.getAppointmentDate());
					preparedStatement.setString(11, appointment.getDoctorName());
					preparedStatement.setString(12, appointment.getHospital());
					preparedStatement.setString(13, appointment.getInsuranceProvider());
					preparedStatement.setString(14, appointment.getPaymentMethod());
				},
				execOptions,
				connOptions
		)).name("Insert into Appointments table sink");
		// env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
		env.execute("Flink Healthcare Realtime Streaming");
	}
}
