FROM gradle:5.4.1-jdk8

RUN mkdir -p /tmp/app
WORKDIR /tmp/app

# Copy project
COPY gradlew .
COPY gradle gradle
COPY build.gradle .
COPY src src

# change ownership of folder
USER root
RUN chown -R gradle /tmp/app
USER gradle

# run gradle wrapper and build
RUN gradle wrapper --info
RUN gradle build --info

CMD ["java","-jar","build/libs/cmps128-4-0.1.0.jar"]
