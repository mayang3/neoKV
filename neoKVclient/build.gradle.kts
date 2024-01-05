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
    implementation(project(mapOf("path" to ":network")))
    testImplementation("junit:junit:4.12")
//    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.8.1")
}


java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}