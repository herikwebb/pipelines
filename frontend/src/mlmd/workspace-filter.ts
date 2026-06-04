/*
 * Copyright 2026 The Kubeflow Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ListOperationOptions } from 'src/third_party/mlmd';
import { ArtifactCustomProperties } from 'src/mlmd/Api';

export function buildWorkspaceFilterQuery(namespace: string): string {
  return `custom_properties.${ArtifactCustomProperties.WORKSPACE}.string_value = "${namespace}"`;
}

export function applyWorkspaceFilterToListOptions(
  listOperationOptions: ListOperationOptions,
  namespace: string | undefined,
): void {
  if (!namespace) {
    return;
  }
  const workspaceFilter = buildWorkspaceFilterQuery(namespace);
  const existingFilter = listOperationOptions.getFilterQuery();
  if (!existingFilter) {
    listOperationOptions.setFilterQuery(workspaceFilter);
    return;
  }
  if (existingFilter.includes(ArtifactCustomProperties.WORKSPACE)) {
    return;
  }
  listOperationOptions.setFilterQuery(`(${existingFilter}) AND (${workspaceFilter})`);
}
