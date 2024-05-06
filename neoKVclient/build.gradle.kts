plugins {
    java
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":network"))
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.20.0")
    implementation("ch.qos.logback:logback-core:1.4.11")
    implementation("ch.qos.logback:logback-classic:1.4.11")
    implementation("commons-io:commons-io:2.14.0")
    implementation("com.google.guava:guava:11.0.2")
    implementation("org.apache.commons:commons-collections4:4.4")
    implementation("org.apache.commons:commons-lang3:3.0")
    implementation("io.netty:netty-all:4.1.24.Final")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.10.2")
    testImplementation("org.junit.vintage:junit-vintage-engine:5.10.2")
}


java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}