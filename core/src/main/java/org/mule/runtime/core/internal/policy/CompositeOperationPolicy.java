/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.core.internal.policy;

import static java.util.Optional.empty;
import static org.mule.runtime.core.privileged.processor.MessageProcessors.processToApply;
import static org.mule.runtime.core.privileged.processor.MessageProcessors.processWithChildContext;
import static reactor.core.publisher.Mono.error;
import static reactor.core.publisher.Mono.from;

import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.core.api.event.CoreEvent;
import org.mule.runtime.core.api.policy.OperationPolicyParametersTransformer;
import org.mule.runtime.core.api.policy.Policy;
import org.mule.runtime.core.api.processor.Processor;
import org.mule.runtime.core.api.processor.ReactiveProcessor;
import org.mule.runtime.core.internal.message.InternalEvent;

import org.reactivestreams.Publisher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * {@link OperationPolicy} created from a list of {@link Policy}.
 * <p>
 * Implements the template methods from {@link AbstractCompositePolicy} required to work with operation policies.
 *
 * @since 4.0
 */
public class CompositeOperationPolicy
    extends AbstractCompositePolicy<OperationPolicyParametersTransformer, OperationExecutionFunction> implements OperationPolicy {

  private static final String POLICY_OPERATION_NEXT_OPERATION_RESPONSE = "policy.operation.nextOperationResponse";

  private final OperationPolicyProcessorFactory operationPolicyProcessorFactory;

  /**
   * Creates a new composite policy.
   * <p>
   * If a non-empty {@code operationPolicyParametersTransformer} is passed to this class, then it will be used to convert the flow
   * execution response parameters to a message with the content of such parameters in order to allow the pipeline after the
   * next-operation to modify the response. If an empty {@code operationPolicyParametersTransformer} is provided then the policy
   * won't be able to change the response parameters of the source and the original response parameters generated from the source
   * will be used.
   *
   * @param parameterizedPolicies list of {@link Policy} to chain together.
   * @param operationPolicyParametersTransformer transformer from the operation parameters to a message and vice versa.
   * @param operationPolicyProcessorFactory factory for creating each {@link OperationPolicy} from a {@link Policy}
   * @param operationExecutionFunction the function that executes the operation.
   */
  public CompositeOperationPolicy(List<Policy> parameterizedPolicies,
                                  Optional<OperationPolicyParametersTransformer> operationPolicyParametersTransformer,
                                  OperationPolicyProcessorFactory operationPolicyProcessorFactory,
                                  OperationExecutionFunction operationExecutionFunction) {
    super(parameterizedPolicies, operationPolicyParametersTransformer, operationExecutionFunction);
    this.operationPolicyProcessorFactory = operationPolicyProcessorFactory;
  }

  @FunctionalInterface
  private interface OperationPolicyNextProcessor extends Processor {

    @Override
    default CoreEvent process(CoreEvent event) throws MuleException {
      return processToApply(event, this);
    }

    @Override
    Publisher<CoreEvent> apply(Publisher<CoreEvent> publisher);

  }

  /**
   * Stores the operation result so all the chains after the operation execution are executed with the actual operation result and
   * not a modified version from another policy.
   *
   * @param eventPub the event to execute the operation.
   */
  @Override
  protected Publisher<CoreEvent> processNextOperation(Publisher<CoreEvent> eventPub,
                                                      OperationExecutionFunction operationExecutionFunction) {
    return from(eventPub)
        .flatMap(event -> {
          OperationParametersProcessor parametersProcessor =
              (OperationParametersProcessor) ((InternalEvent) event).getInternalParameters()
                  .get(POLICY_OPERATION_PARAMETERS_PROCESSOR);

          Map<String, Object> parametersMap = new HashMap<>();
          if (parametersProcessor != null) {
            try {
              parametersMap.putAll(parametersProcessor.getOperationParameters());
            } catch (Exception e) {
              return error(e);
            }
          }
          if (getParametersTransformer().isPresent()) {
            parametersMap.putAll(getParametersTransformer().get().fromMessageToParameters(event.getMessage()));
          }
          return from(operationExecutionFunction.execute(parametersMap, event));
        })
        .map(response -> InternalEvent.builder(response)
            .addInternalParameter(POLICY_OPERATION_NEXT_OPERATION_RESPONSE, response)
            .build())
        .cast(CoreEvent.class);
  }

  /**
   * Always uses the stored result of {@code processNextOperation} so all the chains after the operation execution are executed
   * with the actual operation result and not a modified version from another policy.
   *
   * @param policy the policy to execute.
   * @param nextProcessor the processor to execute when the policy next-processor gets executed
   * @param eventPub the event to use to execute the policy chain.
   */
  @Override
  protected Publisher<CoreEvent> processPolicy(Policy policy, ReactiveProcessor nextProcessor, Publisher<CoreEvent> eventPub) {
    Processor defaultOperationPolicy = operationPolicyProcessorFactory.createOperationPolicy(policy, nextProcessor);
    return from(eventPub)
        .transform(defaultOperationPolicy)
        .map(policyResponse -> {

          if (policy.getPolicyChain().isPropagateMessageTransformations()) {
            return InternalEvent.builder(policyResponse)
                .addInternalParameter(POLICY_OPERATION_NEXT_OPERATION_RESPONSE, policyResponse)
                .build();
          }

          final InternalEvent nextOperationResponse = (InternalEvent) ((InternalEvent) policyResponse).getInternalParameters()
              .get(POLICY_OPERATION_NEXT_OPERATION_RESPONSE);
          return nextOperationResponse != null ? nextOperationResponse : policyResponse;
        })
        .cast(CoreEvent.class);
  }

  @Override
  public Publisher<CoreEvent> process(CoreEvent operationEvent, OperationParametersProcessor parametersProcessor) {
    CoreEvent operationEventForPolicy = InternalEvent.builder(operationEvent)
        .addInternalParameter(POLICY_OPERATION_PARAMETERS_PROCESSOR, parametersProcessor)
        .build();

    try {
      if (getParametersTransformer().isPresent()) {
        return processWithChildContext(CoreEvent.builder(operationEventForPolicy)
            .message(getParametersTransformer().get().fromParametersToMessage(parametersProcessor.getOperationParameters()))
            .build(), getExecutionProcessor(), empty());
      } else {
        return processWithChildContext(operationEventForPolicy, getExecutionProcessor(), empty());
      }
    } catch (Exception e) {
      return error(e);
    }
  }
}
