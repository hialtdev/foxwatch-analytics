plugins {
    java
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "dev.hialt.foxwatch"
version = "1.0.0"

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

repositories {
    mavenCentral()
}

val flinkVersion = "1.19.3"
val kafkaConnectorVersion = "3.3.0-1.19"

dependencies {
    // Flink core — provided at runtime by the Flink cluster
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-clients:$flinkVersion")

    // Kafka connector — bundled into the fat JAR
    implementation("org.apache.flink:flink-connector-kafka:$kafkaConnectorVersion")
    implementation("org.apache.flink:flink-json:$flinkVersion")

    // Jackson for JSON parsing
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.0")

    // Logging — Log4j2 (Flink provides log4j-api, we need the impl and HTTP appender)
    implementation("org.apache.logging.log4j:log4j-core:2.23.1")
    implementation("org.apache.logging.log4j:log4j-api:2.23.1")
    implementation("org.apache.logging.log4j:log4j-slf4j2-impl:2.23.1")

// HTTP appender for Seq — sends CLEF events over HTTP
    implementation("com.lmax:disruptor:3.4.4")
    implementation("org.apache.httpcomponents:httpclient:4.5.14")

    // Testing
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

application {
    mainClass.set("dev.hialt.foxwatch.analytics.DropoutDetectorJob")
}

tasks.shadowJar {
    archiveClassifier.set("")
    mergeServiceFiles()
    manifest {
        attributes["Main-Class"] = "dev.hialt.foxwatch.analytics.DropoutDetectorJob"
    }
}

tasks.test {
    useJUnitPlatform()
}