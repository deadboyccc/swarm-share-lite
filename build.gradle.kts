plugins {
    java
    jacoco
    // Applied only in :cli (see cli/build.gradle.kts); declared here to pin one version.
    id("org.graalvm.buildtools.native") version "1.1.18" apply false
}

allprojects {
    group   = "io.swarmshare"
    version = "0.2.0"
    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java")
    apply(plugin = "jacoco")

    java {
        toolchain {
            languageVersion = JavaLanguageVersion.of(25)
        }
    }

    jacoco {
        toolVersion = "0.8.16"
    }

    dependencies {
        testImplementation("org.junit.jupiter:junit-jupiter:6.1.3")
        testImplementation("org.assertj:assertj-core:3.27.7")
        testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    }

    tasks.test {
        useJUnitPlatform()
        finalizedBy(tasks.named("jacocoTestReport"))
    }

    tasks.named<JacocoReport>("jacocoTestReport") {
        dependsOn(tasks.test)
        reports { xml.required = true }
    }
}

// Hexagonal Boundary Graph: Infrastructure modules explicitly depend on core ports
project(":manifest") { dependencies { implementation(project(":core")); implementation("com.fasterxml.jackson.core:jackson-databind:2.22.4") } }
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
    implementation(project(":manifest"))
} }
project(":cli")         { dependencies {
    implementation(project(":core"))
    implementation(project(":manifest"))
    implementation(project(":storage"))
    implementation(project(":networking"))
    implementation(project(":transfer"))
    implementation("info.picocli:picocli:4.7.7")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.22.4")
} }

// One coverage number for the whole codebase: each module's unit tests also
// exercise the modules it depends on (core is mostly covered by manifest and
// transfer tests), which per-module reports cannot see.
val jacocoAggregatedReport by tasks.registering(JacocoReport::class) {
    group = "verification"
    description = "Aggregates unit-test coverage of every module into one report."
    dependsOn(subprojects.map { it.tasks.named("test") })
    executionData.from(subprojects.map { it.layout.buildDirectory.file("jacoco/test.exec") })
    sourceDirectories.from(subprojects.map { it.file("src/main/java") })
    classDirectories.from(subprojects.map { it.layout.buildDirectory.dir("classes/java/main") })
    reports {
        xml.required = true
        html.required = true
    }
}
