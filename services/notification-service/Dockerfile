FROM eclipse-temurin:21-jre

WORKDIR /app

COPY services/notification-service/target/notification-service-0.1.0-SNAPSHOT.jar app.jar

USER 10001

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
