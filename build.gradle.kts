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
        testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
        testImplementation("org.assertj:assertj-core:3.26.3")
        testImplementation("org.mockito:mockito-core:5.14.2")
        testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    }

    tasks.test {
        useJUnitPlatform()
    }
}

// Hexagonal Boundary Graph: Infrastructure modules explicitly depend on core ports
project(":manifest")    { dependencies { implementation(project(":core")) } }
project(":storage")     { dependencies { implementation(project(":core")) } }
project(":networking")  { dependencies { implementation(project(":core")) } }
project(":transfer")    { dependencies {
    implementation(project(":core"))
    implementation(project(":storage"))
} }
project(":cli")         { dependencies {
    implementation(project(":core"))
    implementation(project(":manifest"))
    implementation(project(":storage"))
    implementation(project(":networking"))
    implementation(project(":transfer"))
    implementation("info.picocli:picocli:4.7.7")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.1")
} }
