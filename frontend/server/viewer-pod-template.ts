// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const BLOCKED_POD_SPEC_FIELDS = new Set([
  'hostNetwork',
  'hostPID',
  'hostIPC',
  'hostUsers',
  'shareProcessNamespace',
  'securityContext',
  'serviceAccountName',
  'serviceAccount',
  'automountServiceAccountToken',
  'initContainers',
  'ephemeralContainers',
  'overhead',
  'schedulingGates',
  'resourceClaims',
]);

const ALLOWED_VOLUME_TYPES = new Set([
  'emptyDir',
  'persistentVolumeClaim',
  'configMap',
  'secret',
  'projected',
  'downwardAPI',
]);

const ALLOWED_CONTAINER_FIELDS = new Set(['resources', 'env', 'volumeMounts']);

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function deepClone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function sanitizeVolumes(volumes: unknown): Record<string, unknown>[] | undefined {
  if (!Array.isArray(volumes)) {
    return undefined;
  }

  const sanitizedVolumes: Record<string, unknown>[] = [];
  for (const volume of volumes) {
    if (!isPlainObject(volume) || typeof volume.name !== 'string' || volume.name.length === 0) {
      throw new Error('podtemplatespec volumes must use non-empty names');
    }

    const volumeTypes = Object.keys(volume).filter((key) => key !== 'name');
    if (volumeTypes.length !== 1) {
      throw new Error(`podtemplatespec volume "${volume.name}" must declare exactly one source`);
    }

    const volumeType = volumeTypes[0];
    if (!ALLOWED_VOLUME_TYPES.has(volumeType)) {
      throw new Error(`podtemplatespec volume type "${volumeType}" is not allowed`);
    }

    sanitizedVolumes.push({
      name: volume.name,
      [volumeType]: volume[volumeType],
    });
  }

  return sanitizedVolumes;
}

function sanitizeContainer(container: unknown): Record<string, unknown> {
  if (!isPlainObject(container)) {
    throw new Error('podtemplatespec containers[0] must be an object');
  }

  const sanitizedContainer: Record<string, unknown> = {};
  for (const field of Object.keys(container)) {
    if (!ALLOWED_CONTAINER_FIELDS.has(field)) {
      throw new Error(`podtemplatespec containers[0].${field} is not allowed`);
    }
    sanitizedContainer[field] = container[field];
  }

  if (isPlainObject(container.securityContext)) {
    throw new Error('podtemplatespec containers[0].securityContext is not allowed');
  }

  return sanitizedContainer;
}

/**
 * Merges a user-supplied Viewer pod template with the deployment default while
 * blocking fields that would let a namespace user escalate to host access or
 * run arbitrary sidecar workloads.
 */
export function sanitizeViewerPodTemplateSpec(
  userSpec: unknown,
  defaultSpec: object,
): Record<string, unknown> {
  const sanitizedSpec = deepClone(defaultSpec) as Record<string, unknown>;
  if (!userSpec) {
    return sanitizedSpec;
  }

  if (!isPlainObject(userSpec)) {
    throw new Error('podtemplatespec must be a JSON object');
  }

  const userPodSpec = userSpec.spec;
  if (userPodSpec === undefined) {
    return sanitizedSpec;
  }
  if (!isPlainObject(userPodSpec)) {
    throw new Error('podtemplatespec.spec must be an object');
  }

  for (const blockedField of BLOCKED_POD_SPEC_FIELDS) {
    if (blockedField in userPodSpec) {
      throw new Error(`podtemplatespec.spec.${blockedField} is not allowed`);
    }
  }

  const sanitizedPodSpec = isPlainObject(sanitizedSpec.spec)
    ? (sanitizedSpec.spec as Record<string, unknown>)
    : {};

  if ('nodeSelector' in userPodSpec) {
    sanitizedPodSpec.nodeSelector = userPodSpec.nodeSelector;
  }
  if ('tolerations' in userPodSpec) {
    sanitizedPodSpec.tolerations = userPodSpec.tolerations;
  }
  if ('affinity' in userPodSpec) {
    sanitizedPodSpec.affinity = userPodSpec.affinity;
  }

  if ('volumes' in userPodSpec) {
    sanitizedPodSpec.volumes = sanitizeVolumes(userPodSpec.volumes);
  }

  if ('containers' in userPodSpec) {
    if (!Array.isArray(userPodSpec.containers) || userPodSpec.containers.length !== 1) {
      throw new Error('podtemplatespec.spec.containers must contain exactly one container');
    }
    const defaultContainers = Array.isArray(sanitizedPodSpec.containers)
      ? (sanitizedPodSpec.containers as Record<string, unknown>[])
      : [{}];
    const defaultContainer = isPlainObject(defaultContainers[0]) ? defaultContainers[0] : {};
    sanitizedPodSpec.containers = [
      {
        ...defaultContainer,
        ...sanitizeContainer(userPodSpec.containers[0]),
      },
    ];
  }

  sanitizedSpec.spec = sanitizedPodSpec;
  return sanitizedSpec;
}
