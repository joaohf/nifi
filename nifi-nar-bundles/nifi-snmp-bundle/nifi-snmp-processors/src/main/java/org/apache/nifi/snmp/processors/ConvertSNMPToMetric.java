/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.snmp.processors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.search.SearchContext;
import org.apache.nifi.search.SearchResult;
import org.apache.nifi.search.Searchable;
import org.apache.nifi.stream.io.BufferedOutputStream;

/**
 * This processor supports converting a SNMP flowfile attributes to JSON
 *
 * This processor only supports a SUCCESS relationship.
 *
 */
@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"attributes", "modification", "update", "delete"})
@CapabilityDescription("Updates the Attributes for a FlowFile by using the Attribute Expression Language and/or deletes the attributes based on a regular expression")
@DynamicProperty(name = "A FlowFile attribute to update", value = "The value to set it to", supportsExpressionLanguage = true,
        description = "Updates a FlowFile attribute specified by the Dynamic Property's key with the value specified by the Dynamic Property's value")
@WritesAttribute(attribute = "See additional details", description = "This processor may write or remove zero or more attributes as described in additional details")
public class ConvertSNMPToMetric extends AbstractProcessor {

    private final ConcurrentMap<String, PropertyValue> propertyValues = new ConcurrentHashMap<>();

    private final Set<Relationship> relationships;
    private volatile List<PropertyDescriptor> customAttributes = new ArrayList<>();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String APPLICATION_JSON = "application/json";

    private static final Validator DELETE_PROPERTY_VALIDATOR = new Validator() {
        private final Validator DPV_RE_VALIDATOR = StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true);

        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                final AttributeExpression.ResultType resultType = context.newExpressionLanguageCompiler().getResultType(input);
                if (!resultType.equals(AttributeExpression.ResultType.STRING)) {
                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(input)
                            .valid(false)
                            .explanation("Expected property to to return type " + AttributeExpression.ResultType.STRING +
                                    " but expression returns type " + resultType)
                            .build();
                }
                return new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(true)
                        .explanation("Property returns type " + AttributeExpression.ResultType.STRING)
                        .build();
            }

            return DPV_RE_VALIDATOR.validate(subject, input, context);
        }
    };

    // static properties
    public static final PropertyDescriptor DELETE_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Delete Attributes Expression")
            .description("Regular expression for attributes to be deleted from flowfiles.")
            .required(false)
            .addValidator(DELETE_PROPERTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor INCLUDE_CORE_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Include Core Attributes")
            .description("Determines if the FlowFile org.apache.nifi.flowfile.attributes.CoreAttributes which are " +
                    "contained in every FlowFile should be included in the final JSON value generated.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor NULL_VALUE_FOR_EMPTY_STRING = new PropertyDescriptor.Builder()
            .name(("Null Value"))
            .description("If true a non existing or empty attribute will be NULL in the resulting JSON. If false an empty " +
                    "string will be placed in the JSON")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    // relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .description("All FlowFiles are routed to this relationship").name("success").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to convert attributes to JSON").build();

    public ConvertSNMPToMetric() {
        final Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_SUCCESS);
        relationshipSet.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationshipSet);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DELETE_ATTRIBUTES);
        descriptors.add(INCLUDE_CORE_ATTRIBUTES);
        descriptors.add(NULL_VALUE_FOR_EMPTY_STRING);
        return Collections.unmodifiableList(descriptors);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }

    @OnStopped
    public final void cleanUpClient() {
        this.customAttributes.clear();
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) throws ProcessException {
        if (customAttributes.size() == 0) {
            for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
                // only custom defined properties
                if (!getSupportedPropertyDescriptors().contains(property.getKey())) {
                    customAttributes.add(property.getKey());
                }
            }
        }
    }

    /**
     * Builds the Map of attributes that should be included in the JSON that is emitted from this process.
     *
     * @return Map of values that are feed to a Jackson ObjectMapper
     */
    private FlowFile buildAttributesMapForFlowFile(final ProcessSession session, FlowFile ff,
                                                              final ProcessContext context,
                                                              List<PropertyDescriptor> customProperties) {
        Map<String, String> attributesToUpdate = new HashMap<>();

        attributesToUpdate.putAll(ff.getAttributes());

        PropertyValue customAttributeValue;
        for (PropertyDescriptor customProperty : customProperties) {
            customAttributeValue = context.getProperty(customProperty).evaluateAttributeExpressions(ff);
            if (StringUtils.isNotBlank(customAttributeValue.getValue())) {
                attributesToUpdate.put(customProperty.getName(), customAttributeValue.getValue());
            }
        }

        // update the flowfile attributes
        return session.putAllAttributes(ff, attributesToUpdate);
    }

    private void buildAttributesListForJson(final Map<String, Object> ffAttributes,
                                            final String regexToDelete,
                                            boolean includeCoreAttributes) {
        final ComponentLog logger = getLogger();
        final Set<String> atsToDelete = new HashSet<>();

        try {
            if (regexToDelete != null) {
                Pattern pattern = Pattern.compile(regexToDelete);
                final Set<String> attributeKeys = ffAttributes.keySet();
                for (final String key : attributeKeys) {
                    if (pattern.matcher(key).matches()) {

                        // log if appropriate
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("%s deleting attribute '%s' for regex '%s'.", this,
                                    key, regexToDelete));
                        }

                        atsToDelete.add(key);
                    }
                }

                for (final String key: atsToDelete) {
                    ffAttributes.remove(key);
                }
            }
        } catch (final ProcessException pe) {
            throw new ProcessException(String.format("Unable to delete attribute '%s': %s.",
                    regexToDelete, pe), pe);
        }

        if (!includeCoreAttributes) {
            removeCoreAttributes(ffAttributes);
        }
    }

    private Map<String, Object> buildValueForMetric(final Map<String, String> ats) {
        return SNMPUtils.getValueFromAttributes(ats);
    }

    /**
     * Remove all of the CoreAttributes from the Attributes that will be written to the Flowfile.
     *
     * @param ffAttributes Map of Attributes that have already been generated including the CoreAttributes
     * @return none
     */
    private void removeCoreAttributes(final Map<String, Object> ffAttributes) {
        //Map<String, Object> ats = new HashMap<>();

        //ats.putAll(ffAttributes);

        for (CoreAttributes c : CoreAttributes.values()) {
            ffAttributes.remove(c.key());
        }

        //return ats;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final ComponentLog logger = getLogger();

        List<FlowFile> flowFiles = session.get(100);
        if (flowFiles.isEmpty()) {
            return;
        }

        for (FlowFile flowFile : flowFiles) {

            // add custom attributes
            final FlowFile ff = buildAttributesMapForFlowFile(session, flowFile, context, customAttributes);

            // build metrics
            final Map<String, Object> valueList = buildValueForMetric(ff.getAttributes());

            // delete unused fields
            buildAttributesListForJson(valueList,
                    context.getProperty(DELETE_ATTRIBUTES).evaluateAttributeExpressions(ff).getValue(),
                    context.getProperty(INCLUDE_CORE_ATTRIBUTES).asBoolean());

            FlowFile conFlowfile = session.write(ff, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    try (OutputStream outputStream = new BufferedOutputStream(out)) {
                        outputStream.write(objectMapper.writeValueAsBytes(valueList));
                    } catch (JsonProcessingException e) {
                        getLogger().error(e.getMessage());
                        session.transfer(ff, REL_FAILURE);
                    }
                }
            });
            conFlowfile = session.putAttribute(conFlowfile, CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);

            //logger.info("Updated attributes for {}; transferring to '{}'", new Object[]{ff, REL_SUCCESS.getName()});
            session.getProvenanceReporter().modifyAttributes(ff);
            session.transfer(conFlowfile, REL_SUCCESS);
        }
    }

    private PropertyValue getPropertyValue(final String text, final ProcessContext context) {
        PropertyValue currentValue = propertyValues.get(text);
        if (currentValue == null) {
            currentValue = context.newPropertyValue(text);
            PropertyValue previousValue = propertyValues.putIfAbsent(text, currentValue);
            if (previousValue != null) {
                currentValue = previousValue;
            }
        }

        return currentValue;
    }
}