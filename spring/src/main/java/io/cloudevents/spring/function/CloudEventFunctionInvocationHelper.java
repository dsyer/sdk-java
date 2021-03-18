/*
 * Copyright 2019-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.cloudevents.spring.function;

import java.util.Collections;

import org.springframework.beans.BeansException;
import org.springframework.cloud.function.core.FunctionInvocationHelper;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.ClassUtils;

import io.cloudevents.CloudEvent;
import io.cloudevents.spring.messaging.CloudEventMessageConverter;

public class CloudEventFunctionInvocationHelper
        implements FunctionInvocationHelper<Message<?>>, ApplicationContextAware {

    private FunctionInvocationHelper<Message<?>> delegate;
    private CloudEventMessageConverter converter = new CloudEventMessageConverter();

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        @SuppressWarnings("unchecked")
        FunctionInvocationHelper<Message<?>> delegate = (FunctionInvocationHelper<Message<?>>) applicationContext
                .getAutowireCapableBeanFactory().createBean(ClassUtils.resolveClassName(
                        "org.springframework.cloud.function.cloudevent.CloudEventsFunctionInvocationHelper", null));
        this.delegate = delegate;
        ((ApplicationContextAware) delegate).setApplicationContext(applicationContext);
    }

    @Override
    public boolean isRetainOuputAsMessage(Message<?> input) {
        return delegate.isRetainOuputAsMessage(input);
    }

    @Override
    public Message<?> preProcessInput(Message<?> input, Object inputConverter) {
        return delegate.preProcessInput(input, inputConverter);
    }

    @Override
    public Message<?> postProcessResult(Object result, String hint) {
        return delegate.postProcessResult(result, hint);
    }

    @Override
    public Message<?> postProcessResult(Object result, Message<?> input) {
        if (result instanceof CloudEvent) {
            result = converter.toMessage(result, input.getHeaders());
        }
        return delegate.postProcessResult(result, input);
    }
}
