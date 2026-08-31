plugins {
    java
}

allprojects {
    group   = "io.swarmshare"
    version = "0.1.0-SNAPSHOT"
    repositories { 
        mavenCentral() 
    }
}

subprojects {
    apply(plugin = "java")

    java {
        toolchain {
            languageVersion = JavaLanguageVersion.of(25)
        }
    }

    dependencies {
        testImplementation("org.junit.jupiter:junit-jupiter:6.1.3")
        testImplementation("org.assertj:assertj-core:3.27.7")
        testImplementation("org.mockito:mockito-core:5.23.0")
        testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    }

    tasks.test {
        useJUnitPlatform()
    }
}

// Hexagonal Boundary Graph: Infrastructure modules explicitly depend on core ports
project(":manifest") { dependencies { implementation(project(":core")); implementation("com.fasterxml.jackson.core:jackson-databind:2.22.2") } }
project(":storage")     { dependencies { implementation(project(":core")) } }
project(":networking") {
    dependencies {
        implementation(project(":core"))
        testImplementation(project(":manifest"))
        testImplementation(project(":storage"))
        testImplementation(project(":transfer"))
    }
}
project(":transfer")    { dependencies {
    implementation(project(":core"))
    implementation(project(":storage"))
    testImplementation(project(":manifest"))
} }
project(":cli")         { dependencies {
    implementation(project(":core"))
    implementation(project(":manifest"))
    implementation(project(":storage"))
    implementation(project(":networking"))
    implementation(project(":transfer"))
    implementation("info.picocli:picocli:4.7.7")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.22.2")
} }
