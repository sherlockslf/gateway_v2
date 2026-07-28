package com.usig.testkit;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

class ArchitectureRulesTest {

  private static final JavaClasses CLASSES =
      new ClassFileImporter()
          .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
          .importPackages("com.usig");

  @Test
  void domainDoesNotDependOnFrameworksOrInfrastructure() {
    noClasses()
        .that()
        .resideInAPackage("com.usig.domain..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage(
            "io.quarkus..",
            "io.vertx..",
            "io.smallrye..",
            "io.redis..",
            "org.apache.kafka..",
            "jakarta.ws.rs..")
        .check(CLASSES);
  }

  @Test
  void applicationDoesNotDependOnAdaptersInfrastructureOrApi() {
    noClasses()
        .that()
        .resideInAPackage("com.usig.application..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage("com.usig.adapter..", "com.usig.infra..", "com.usig.api..")
        .check(CLASSES);
  }

  @Test
  void adaptersDoNotDependOnApiTransport() {
    noClasses()
        .that()
        .resideInAPackage("com.usig.adapter..")
        .should()
        .dependOnClassesThat()
        .resideInAPackage("com.usig.api..")
        .check(CLASSES);
  }

  @Test
  void apiTransportDoesNotDependOnDownstreamOrInfrastructureImplementations() {
    noClasses()
        .that()
        .resideInAPackage("com.usig.api..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage("com.usig.adapter..", "com.usig.infra..")
        .check(CLASSES);
  }

  @Test
  void resourceClassesOnlyLiveInApiModule() {
    classes()
        .that()
        .haveSimpleNameEndingWith("Resource")
        .should()
        .resideInAPackage("com.usig.api..")
        .check(CLASSES);
  }
}
