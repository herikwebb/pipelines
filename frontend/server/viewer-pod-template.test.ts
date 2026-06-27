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
import { sanitizeViewerPodTemplateSpec } from './viewer-pod-template.js';

const defaultPodTemplateSpec = {
  spec: {
    serviceAccountName: 'default-editor',
    containers: [{}],
  },
};

describe('sanitizeViewerPodTemplateSpec', () => {
  it('returns the default template when user input is omitted', () => {
    expect(sanitizeViewerPodTemplateSpec(undefined, defaultPodTemplateSpec)).toEqual(
      defaultPodTemplateSpec,
    );
  });

  it('merges allowed scheduling and container overrides', () => {
    const userSpec = {
      spec: {
        nodeSelector: { 'cloud.google.com/gke-accelerator': 'nvidia-tesla-t4' },
        containers: [
          {
            resources: {
              limits: { 'nvidia.com/gpu': '1' },
            },
            env: [{ name: 'AWS_REGION', value: 'minio' }],
          },
        ],
        volumes: [
          {
            name: 'artifacts',
            secret: {
              secretName: 'mlpipeline-minio-artifact',
            },
          },
        ],
      },
    };

    expect(sanitizeViewerPodTemplateSpec(userSpec, defaultPodTemplateSpec)).toEqual({
      spec: {
        serviceAccountName: 'default-editor',
        nodeSelector: { 'cloud.google.com/gke-accelerator': 'nvidia-tesla-t4' },
        containers: [
          {
            resources: {
              limits: { 'nvidia.com/gpu': '1' },
            },
            env: [{ name: 'AWS_REGION', value: 'minio' }],
          },
        ],
        volumes: [
          {
            name: 'artifacts',
            secret: {
              secretName: 'mlpipeline-minio-artifact',
            },
          },
        ],
      },
    });
  });

  it('rejects hostPath volumes', () => {
    expect(() =>
      sanitizeViewerPodTemplateSpec(
        {
          spec: {
            volumes: [{ name: 'host', hostPath: { path: '/' } }],
          },
        },
        defaultPodTemplateSpec,
      ),
    ).toThrow('podtemplatespec volume type "hostPath" is not allowed');
  });

  it('rejects host namespace and privilege escalation fields', () => {
    expect(() =>
      sanitizeViewerPodTemplateSpec(
        {
          spec: {
            hostNetwork: true,
          },
        },
        defaultPodTemplateSpec,
      ),
    ).toThrow('podtemplatespec.spec.hostNetwork is not allowed');

    expect(() =>
      sanitizeViewerPodTemplateSpec(
        {
          spec: {
            containers: [{ securityContext: { privileged: true } }],
          },
        },
        defaultPodTemplateSpec,
      ),
    ).toThrow('podtemplatespec containers[0].securityContext is not allowed');
  });

  it('rejects extra containers and init containers', () => {
    expect(() =>
      sanitizeViewerPodTemplateSpec(
        {
          spec: {
            containers: [{}, {}],
          },
        },
        defaultPodTemplateSpec,
      ),
    ).toThrow('podtemplatespec.spec.containers must contain exactly one container');

    expect(() =>
      sanitizeViewerPodTemplateSpec(
        {
          spec: {
            initContainers: [{ name: 'sidecar', image: 'busybox' }],
          },
        },
        defaultPodTemplateSpec,
      ),
    ).toThrow('podtemplatespec.spec.initContainers is not allowed');
  });

  it('rejects service account overrides', () => {
    expect(() =>
      sanitizeViewerPodTemplateSpec(
        {
          spec: {
            serviceAccountName: 'default-admin',
          },
        },
        defaultPodTemplateSpec,
      ),
    ).toThrow('podtemplatespec.spec.serviceAccountName is not allowed');
  });
});
