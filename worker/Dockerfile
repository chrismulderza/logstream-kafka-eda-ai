# Multi-stage Red Hat UBI 9 + OpenJDK 21 image for the predictive AI stream worker.
# Matches Quarkus 3.40 defaults for Java 21 JVM containers (openjdk-21*:1.24).
# Native micro runtime (separate Dockerfile) uses quay.io/quarkus/ubi9-quarkus-micro-image:2.0.
# Build (from worker/):
#   podman build -t IMAGE_REF -f Dockerfile .
FROM registry.access.redhat.com/ubi9/openjdk-21:1.24 AS build

USER root
WORKDIR /build

COPY pom.xml .
COPY mvnw .
COPY .mvn .mvn
COPY src src

RUN chmod +x mvnw \
    && ./mvnw -B -DskipTests package \
    && mkdir -p /build/quarkus-app \
    && cp -r target/quarkus-app/. /build/quarkus-app/

FROM registry.access.redhat.com/ubi9/openjdk-21-runtime:1.24

ENV LANGUAGE='en_US:en' \
    HOME=/tmp \
    JAVA_OPTS_APPEND="-Dquarkus.http.host=0.0.0.0 -Djava.util.logging.manager=org.jboss.logmanager.LogManager" \
    JAVA_APP_JAR="/deployments/quarkus-run.jar"

COPY --from=build --chown=185:0 /build/quarkus-app/lib/ /deployments/lib/
COPY --from=build --chown=185:0 /build/quarkus-app/*.jar /deployments/
COPY --from=build --chown=185:0 /build/quarkus-app/app/ /deployments/app/
COPY --from=build --chown=185:0 /build/quarkus-app/quarkus/ /deployments/quarkus/

EXPOSE 8080
USER 185

ENTRYPOINT [ "/opt/jboss/container/java/run/run-java.sh" ]
