/*
 * Copyright 2020-Present The CloudEvents Authors.
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

package io.cloudevents.spring.messaging;

public class CloudEventsHeaders {

	public static final String CE_PREFIX = "ce-";

	public static final String SPEC_VERSION = CE_PREFIX + "specversion";

	public static final String CONTENT_TYPE = CE_PREFIX + "datacontenttype";

}
