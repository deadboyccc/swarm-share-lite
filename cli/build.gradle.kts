// CLI packaging: a JVM distribution (application plugin), a GraalVM native
// executable, and an end-to-end suite that runs either one as a real process.
// Module dependencies are declared with the rest of the graph in the root build.

plugins {
    application
    id("org.graalvm.buildtools.native")
}

application {
    mainClass = "io.swarmshare.cli.SwarmShareCli"
    applicationName = "swarm-share"
}

dependencies {
    // Generates GraalVM reflection metadata for the @Command classes at compile time.
    annotationProcessor("info.picocli:picocli-codegen:4.7.7")
}

tasks.compileJava {
    options.compilerArgs.add("-Aproject=${project.group}/swarm-share")
}

graalvmNative {
    // Pulls community-maintained metadata (e.g. for Jackson) from the GraalVM
    // reachability-metadata repository; our own classes ship their metadata
    // under src/main/resources/META-INF/native-image.
    metadataRepository {
        enabled = true
    }
    binaries {
        named("main") {
            imageName = "swarm-share"
            mainClass = application.mainClass
            // Fallback images were removed in GraalVM 25, so no --no-fallback.
            buildArgs.add("-H:+ReportExceptionStackTraces")
        }
    }
}

// ── end-to-end tests ──────────────────────────────────────────────────────────
// Black-box tests that launch the packaged CLI as separate OS processes. The
// executable under test is passed in as the `swarm.cli` system property, so the
// same suite verifies both the JVM distribution and the native binary.

val installedCli = layout.buildDirectory.file("install/swarm-share/bin/swarm-share")
val nativeCli = layout.buildDirectory.file("native/nativeCompile/swarm-share")

testing {
    suites {
        register<JvmTestSuite>("e2eTest") {
            useJUnitJupiter("6.1.3")
            dependencies {
                implementation("org.assertj:assertj-core:3.27.7")
                // Runs seeder and leechers as separate hosts on a Docker network (LanSwarmTest).
                implementation("org.testcontainers:testcontainers:2.0.5")
            }
            targets.all {
                testTask.configure {
                    description = "Runs the end-to-end suite against the JVM distribution."
                    shouldRunAfter(tasks.test)
                    dependsOn(tasks.installDist)
                    inputs.files(tasks.installDist)
                    systemProperty("swarm.cli", installedCli.get().asFile.absolutePath)
                    jvmArgs("--enable-native-access=ALL-UNNAMED") // Testcontainers' JNA
                }
            }
        }
    }
}

val e2eSourceSet = sourceSets["e2eTest"]

tasks.register<Test>("nativeE2eTest") {
    description = "Runs the end-to-end suite against the GraalVM native executable."
    group = "verification"
    testClassesDirs = e2eSourceSet.output.classesDirs
    classpath = e2eSourceSet.runtimeClasspath
    useJUnitPlatform()
    dependsOn(tasks.named("nativeCompile"))
    inputs.files(tasks.named("nativeCompile"))
    systemProperty("swarm.cli", nativeCli.get().asFile.absolutePath)
    jvmArgs("--enable-native-access=ALL-UNNAMED") // Testcontainers' JNA
}

tasks.check {
    dependsOn(testing.suites.named("e2eTest"))
}
