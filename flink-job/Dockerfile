# Use Maven to build the JAR
FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /app
COPY . .
RUN mvn clean package -DskipTests

# Use Flink as the base for the final image
FROM flink:1.17.1-scala_2.12-java11

# Copy the built JAR into the Flink job directory
COPY --from=builder /app/target/taxi-flink-job-1.0-SNAPSHOT.jar /opt/flink/usrlib/
