// (c) Copyright 2015 WibiData, Inc.

package testing;

import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;

/**
 * Utility class for gathering the list of tests in test classes.
 */
@SupportedAnnotationTypes("org.junit.Test")
public class TestAnnotationCollector extends AbstractProcessor {

  @Override
  public SourceVersion getSupportedSourceVersion() {
    return SourceVersion.RELEASE_7;
  }

  @Override
  public Set<String> getSupportedAnnotationTypes() {
    final Set<String> types = super.getSupportedAnnotationTypes();
    for (String type: types) {
      System.out.println("Handling annotation : " + type);
    }
    return types;
  }

  @Override
  public synchronized void init(ProcessingEnvironment processingEnv) {
    super.init(processingEnv);
    System.out.println("Test annotation processor initialized.");
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    for (final Element element : roundEnv.getElementsAnnotatedWith(org.junit.Test.class)) {
      switch (element.getKind()) {
        case METHOD: {
          final Element methodElement = element;
          final Element classElement = methodElement.getEnclosingElement();

          if (!classElement.getModifiers().contains(Modifier.PUBLIC)) {
            processingEnv.getMessager()
                .printMessage(Kind.ERROR, "Test class must be public", classElement);
          }

          System.out.println("Test: " + classElement + "." + methodElement);

          methodElement.getModifiers().contains(Modifier.STATIC);
          break;
        }
        default: {
          // Ignore
        }
      }
    }
    return true;
  }

}
