/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.processor;

import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserFunction;

import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.util.Elements;
import javax.lang.model.util.SimpleElementVisitor9;
import javax.tools.Diagnostic;
import java.util.Optional;

public class SignatureVisitor extends SimpleElementVisitor9<String, Void> {

    private final Elements elementUtils;

    private final Messager messager;

    public SignatureVisitor(Elements elementUtils,
                            Messager messager) {

        this.elementUtils = elementUtils;
        this.messager = messager;
    }

    @Override
    public String visitExecutable(ExecutableElement method, Void unused) {
        return getAnnotationName(method)
                .orElse(String.format("%s.%s", elementUtils.getPackageOf(method), method.getSimpleName()));
    }

    @Override
    public String visitUnknown(Element e, Void unused) {
        messager.printMessage(Diagnostic.Kind.ERROR, "unexpected .....");
        return super.visitUnknown(e, unused);
    }

    private Optional<String> getAnnotationName(ExecutableElement method) {
        return getProcedureName(method)
                .or(() -> getUserFunctionName(method))
                .or(() -> getUserAggregationFunctionName(method));
    }

    private Optional<String> getProcedureName(ExecutableElement method) {
        return Optional.ofNullable(method.getAnnotation(Procedure.class))
                .map((annotation) -> pickFirstNonBlank(annotation.name(), annotation.value()))
                .flatMap(this::blankToEmpty);
    }

    private Optional<String> getUserFunctionName(ExecutableElement method) {
        return Optional.ofNullable(method.getAnnotation(UserFunction.class))
                .map((annotation) -> pickFirstNonBlank(annotation.name(), annotation.value()))
                .flatMap(this::blankToEmpty);
    }

    private Optional<String> getUserAggregationFunctionName(ExecutableElement method) {
        return Optional.ofNullable(method.getAnnotation(UserAggregationFunction.class))
                .map((annotation) -> pickFirstNonBlank(annotation.name(), annotation.value()))
                .flatMap(this::blankToEmpty);
    }

    private Optional<String> blankToEmpty(String s) {
        return s.isBlank() ? Optional.empty() : Optional.of(s);
    }

    private String pickFirstNonBlank(String name, String value) {
        return name.isBlank() ? value : name;
    }
}
