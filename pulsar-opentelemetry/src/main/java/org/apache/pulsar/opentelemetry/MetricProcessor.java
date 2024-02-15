/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.opentelemetry;

import com.google.auto.service.AutoService;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;

@SupportedAnnotationTypes("org.apache.pulsar.opentelemetry.Metric")
@SupportedSourceVersion(javax.lang.model.SourceVersion.RELEASE_8)
@AutoService(Processor.class)
public class MetricProcessor extends AbstractProcessor {
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, "DMISCA Processing annotations");
        for (var annotation: annotations) {
            var elements = roundEnv.getElementsAnnotatedWith(annotation);
            try {
                var fileObject = processingEnv.getFiler().createSourceFile("dmisca-metric-processor-output");
                try (var ignored = fileObject.openWriter()) {
                    for (var element : elements) {
                        processingEnv.getMessager().printMessage(
                            Diagnostic.Kind.NOTE, "DMISCA Found annotated element: " + element, element);
                        ignored.write("DMISCA Found annotated element: " + element);
                        ignored.write('\n');
                    }
                }
            } catch (Exception e) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getMessage());
            }
        }
        return true;
    }
}
