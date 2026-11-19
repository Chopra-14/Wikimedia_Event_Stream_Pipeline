FROM maven:3.9.7-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn -B package -DskipTests

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /app/target/streams-app-1.0-jar-with-dependencies.jar ./streams-app.jar
CMD ["java","-jar","streams-app.jar"]