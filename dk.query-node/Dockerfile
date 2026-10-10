# Multi-architecture index digest for maven:3.9.11-eclipse-temurin-21.
FROM --platform=$BUILDPLATFORM maven:3.9.11-eclipse-temurin-21@sha256:6fdc855a6ed81d288ca7ca37ac6ff5e9308b612485c0801d70b25a858c83d237 AS build
WORKDIR /app
COPY . .
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp -DskipTests clean package -pl dk.query-node -am

# Multi-architecture index digest for eclipse-temurin:21-jre-jammy.
FROM eclipse-temurin:21-jre-jammy@sha256:f04fb34e053148344e83317976114ec3f37e4b830ec8bdab5a2fe3cecd7d010b
RUN groupadd --gid 10001 dsearch \
    && useradd --uid 10001 --gid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin dsearch \
    && install --directory --owner=10001 --group=10001 /var/cache/dsearch
WORKDIR /app
COPY --from=build --chown=10001:10001 /app/dk.query-node/target/dk-query-node.jar app.jar

EXPOSE 6000 6100
USER 10001:10001
ENTRYPOINT ["java", "-jar", "app.jar"]
