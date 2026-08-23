// Copyright 2019 The Kubeflow Authors
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
import { vi, describe, it, expect, beforeEach, Mock } from 'vitest';
import { PassThrough } from 'stream';
import { Client as MinioClient } from 'minio';
import {
  createPodLogsMinioRequestConfig,
  composePodLogsStreamHandler,
  getPodLogsStreamFromK8s,
  getPodLogsStreamFromWorkflow,
  configuredArtifactEndpoints,
  toGetPodLogsStream,
  getKeyFormatFromArtifactRepositories,
} from './workflow-helper.js';
import {
  getK8sSecret,
  getArgoWorkflow,
  getPodLogs,
  getConfigMap,
  getServerNamespace,
} from './k8s-helper.js';
import { V1ConfigMap, V1ObjectMeta } from '@kubernetes/client-node';

vi.mock('minio');
vi.mock('./k8s-helper');

describe('workflow-helper', () => {
  const minioConfig = {
    accessKey: 'minio',
    endPoint: 'seaweedfs.kubeflow',
    secretKey: 'minio123',
  };

  beforeEach(() => {
    vi.resetAllMocks();
  });

  describe('configuredArtifactEndpoints', () => {
    it.each(['seaweedfs.kubeflow', 'localhost'])(
      'preserves an already qualified MINIO_HOST value %s',
      (minioHost) => {
        expect(
          configuredArtifactEndpoints({
            MINIO_HOST: minioHost,
            MINIO_NAMESPACE: 'kubeflow',
            MINIO_PORT: '9000',
            MINIO_SSL: 'false',
          }),
        ).toContain(`http://${minioHost}:9000`);
      },
    );

    it('does not trust the AWS SDK default when AWS is not explicitly configured', () => {
      expect(configuredArtifactEndpoints({})).not.toContain('https://s3.amazonaws.com');
    });

    it('trusts an explicitly configured AWS endpoint', () => {
      expect(
        configuredArtifactEndpoints({ AWS_S3_ENDPOINT: 's3.example.test', AWS_SSL: 'true' }),
      ).toContain('https://s3.example.test:443');
    });
  });

  describe('composePodLogsStreamHandler', () => {
    it('returns the stream from the default handler if there is no errors.', async () => {
      const defaultStream = new PassThrough();
      const defaultHandler = vi.fn((_podName: string, _createdAt: string, _namespace?: string) =>
        Promise.resolve(defaultStream),
      );
      const stream = await composePodLogsStreamHandler(defaultHandler)(
        'podName',
        '2024-08-13',
        'namespace',
      );
      expect(defaultHandler).toBeCalledWith('podName', '2024-08-13', 'namespace');
      expect(stream).toBe(defaultStream);
    });

    it('returns the stream from the fallback handler if there is any error.', async () => {
      const fallbackStream = new PassThrough();
      const defaultHandler = vi.fn((_podName: string, _createdAt: string, _namespace?: string) =>
        Promise.reject('unknown error'),
      );
      const fallbackHandler = vi.fn((_podName: string, _createdAt: string, _namespace?: string) =>
        Promise.resolve(fallbackStream),
      );
      const stream = await composePodLogsStreamHandler(defaultHandler, fallbackHandler)(
        'podName',
        '2024-08-13',
        'namespace',
      );
      expect(defaultHandler).toBeCalledWith('podName', '2024-08-13', 'namespace');
      expect(fallbackHandler).toBeCalledWith('podName', '2024-08-13', 'namespace');
      expect(stream).toBe(fallbackStream);
    });

    it('throws error if both handler and fallback fails.', async () => {
      const defaultHandler = vi.fn((_podName: string, _createdAt: string, _namespace?: string) =>
        Promise.reject('unknown error for default'),
      );
      const fallbackHandler = vi.fn((_podName: string, _createdAt: string, _namespace?: string) =>
        Promise.reject('unknown error for fallback'),
      );
      await expect(
        composePodLogsStreamHandler(defaultHandler, fallbackHandler)(
          'podName',
          '2024-08-13',
          'namespace',
        ),
      ).rejects.toEqual('unknown error for fallback');
    });
  });

  describe('getPodLogsStreamFromK8s', () => {
    it('returns the pod log stream using k8s api.', async () => {
      const mockedGetPodLogs: Mock = getPodLogs as any;
      mockedGetPodLogs.mockResolvedValueOnce('pod logs');

      const stream = await getPodLogsStreamFromK8s('podName', '', 'namespace');
      expect(mockedGetPodLogs).toBeCalledWith('podName', 'namespace', 'main');
      expect(stream.read().toString()).toBe('pod logs');
    });
  });

  describe('toGetPodLogsStream', () => {
    it('wraps a getMinioRequestConfig function to return the corresponding object stream.', async () => {
      const objStream = new PassThrough();
      objStream.end('some fake logs.');

      const client = new MinioClient(minioConfig);
      const mockedClientGetObject: Mock = client.getObject as any;
      mockedClientGetObject.mockResolvedValueOnce(objStream);
      const configs = {
        bucket: 'bucket',
        client,
        key: 'folder/key',
      };
      const createRequest = vi.fn((_podName: string, _createdAt: string, _namespace?: string) =>
        Promise.resolve(configs),
      );
      const stream = await toGetPodLogsStream(createRequest)('podName', '2024-08-13', 'namespace');
      expect(mockedClientGetObject).toBeCalledWith('bucket', 'folder/key');
    });
  });

  describe('getKeyFormatFromArtifactRepositories', () => {
    it('returns a keyFormat string from the artifact-repositories configmap.', async () => {
      const artifactRepositories = {
        'artifact-repositories':
          'archiveLogs: true\n' +
          's3:\n' +
          '  accessKeySecret:\n' +
          '    key: accesskey\n' +
          '    name: mlpipeline-minio-artifact\n' +
          '  bucket: mlpipeline\n' +
          '  endpoint: seaweedfs.kubeflow:9000\n' +
          '  insecure: true\n' +
          '  keyFormat: foo\n' +
          '  secretKeySecret:\n' +
          '    key: secretkey\n' +
          '    name: mlpipeline-minio-artifact',
      };

      const mockedConfigMap: V1ConfigMap = {
        apiVersion: 'v1',
        kind: 'ConfigMap',
        metadata: new V1ObjectMeta(),
        data: artifactRepositories,
        binaryData: {},
      };

      const mockedGetConfigMap: Mock = getConfigMap as any;
      mockedGetConfigMap.mockResolvedValueOnce([mockedConfigMap, undefined]);
      const res = await getKeyFormatFromArtifactRepositories('');
      expect(mockedGetConfigMap).toBeCalledTimes(1);
      expect(res).toEqual('foo');
    });
  });

  describe('createPodLogsMinioRequestConfig', () => {
    it('returns a MinioRequestConfig factory with the provided minioClientOptions, bucket, and prefix.', async () => {
      const mockedClient: Mock = MinioClient as any;
      const requestFunc = await createPodLogsMinioRequestConfig(
        minioConfig,
        'bucket',
        'artifacts/{{workflow.name}}/{{workflow.creationTimestamp.Y}}/{{workflow.creationTimestamp.m}}/{{workflow.creationTimestamp.d}}/{{pod.name}}',
        true,
      );
      const request = await requestFunc(
        'workflow-name-system-container-impl-foo',
        '2024-08-13',
        'namespace',
      );

      expect(mockedClient).toBeCalledWith(minioConfig);
      expect(request.client).toBeInstanceOf(MinioClient);
      expect(request.bucket).toBe('bucket');
      expect(request.key).toBe(
        'artifacts/workflow-name/2024/08/13/workflow-name-system-container-impl-foo/main.log',
      );
    });
  });

  describe('getPodLogsStreamFromWorkflow', () => {
    it('returns a getPodLogsStream function that retrieves an object stream using the workflow status corresponding to the pod name.', async () => {
      const sampleWorkflow = {
        apiVersion: 'argoproj.io/v1alpha1',
        kind: 'Workflow',
        status: {
          artifactRepositoryRef: {
            artifactRepository: {
              archiveLogs: true,
              s3: {
                accessKeySecret: { key: 'accessKey', name: 'accessKeyName' },
                bucket: 'bucket',
                endpoint: 'seaweedfs.kubeflow:9000',
                insecure: true,
                key: 'prefix/workflow-name/workflow-name-system-container-impl-abc/some-artifact.csv',
                secretKeySecret: { key: 'secretKey', name: 'secretKeyName' },
              },
            },
          },
          nodes: {
            'workflow-name-abc': {
              outputs: {
                artifacts: [
                  {
                    name: 'main-logs',
                    s3: {
                      key: 'prefix/workflow-name/workflow-name-system-container-impl-abc/main.log',
                    },
                  },
                ],
              },
            },
          },
        },
      };

      const mockedGetArgoWorkflow: Mock = getArgoWorkflow as any;
      mockedGetArgoWorkflow.mockResolvedValueOnce(sampleWorkflow);

      // The run namespace matches the server's own namespace, so reading the
      // object-store credential Secret is permitted.
      const mockedGetServerNamespace: Mock = getServerNamespace as any;
      mockedGetServerNamespace.mockReturnValue('kubeflow');

      const mockedGetK8sSecret: Mock = getK8sSecret as any;
      mockedGetK8sSecret.mockResolvedValue('someSecret');

      const objStream = new PassThrough();
      const mockedClient: Mock = MinioClient as any;
      // In Vitest, auto-mocked class instances get their own mock methods.
      // Set up prototype mock so new instances inherit it.
      MinioClient.prototype.getObject = vi.fn().mockResolvedValueOnce(objStream) as any;
      objStream.end('some fake logs.');

      const stream = await getPodLogsStreamFromWorkflow(
        'workflow-name-system-container-impl-abc',
        '2024-07-09',
        'kubeflow',
      );

      expect(mockedGetArgoWorkflow).toBeCalledWith('workflow-name', 'kubeflow');

      expect(mockedGetK8sSecret).toBeCalledTimes(2);
      expect(mockedGetK8sSecret).toBeCalledWith('accessKeyName', 'accessKey', 'kubeflow');
      expect(mockedGetK8sSecret).toBeCalledWith('secretKeyName', 'secretKey', 'kubeflow');

      expect(mockedClient).toBeCalledTimes(1);
      expect(mockedClient).toBeCalledWith({
        accessKey: 'someSecret',
        endPoint: 'seaweedfs.kubeflow',
        port: 9000,
        secretKey: 'someSecret',
        useSSL: false,
      });
      // Access the instance created by the constructor to check getObject
      const clientInstance = mockedClient.mock.results[0].value;
      expect(clientInstance.getObject).toBeCalledTimes(1);
      expect(clientInstance.getObject).toBeCalledWith(
        'bucket',
        'prefix/workflow-name/workflow-name-system-container-impl-abc/main.log',
      );
    });

    it('does not read the object-store Secret when the run namespace is not the server namespace (security)', async () => {
      const sampleWorkflow = {
        apiVersion: 'argoproj.io/v1alpha1',
        kind: 'Workflow',
        status: {
          artifactRepositoryRef: {
            artifactRepository: {
              archiveLogs: true,
              s3: {
                accessKeySecret: { key: 'accessKey', name: 'accessKeyName' },
                bucket: 'mlpipeline',
                endpoint: 'seaweedfs.kubeflow:9000',
                insecure: true,
                key: 'prefix/workflow-name/workflow-name-system-container-impl-abc/some-artifact.csv',
                secretKeySecret: { key: 'secretKey', name: 'secretKeyName' },
              },
            },
          },
          nodes: {
            'workflow-name-abc': {
              outputs: {
                artifacts: [
                  {
                    name: 'main-logs',
                    s3: {
                      key: 'artifacts/my-user-namespace/workflow-name/2024/07/09/workflow-name-system-container-impl-abc/main.log',
                    },
                  },
                ],
              },
            },
          },
        },
      };

      const mockedGetArgoWorkflow: Mock = getArgoWorkflow as any;
      mockedGetArgoWorkflow.mockResolvedValueOnce(sampleWorkflow);

      // The run namespace is a customer namespace, different from the server's
      // own namespace, so the credential Secret must NOT be read.
      const mockedGetServerNamespace: Mock = getServerNamespace as any;
      mockedGetServerNamespace.mockReturnValue('kubeflow');

      const mockedGetK8sSecret: Mock = getK8sSecret as any;

      // The server's own object-store credentials are provided via the
      // environment, matching the deployment's MINIO_ACCESS_KEY/MINIO_SECRET_KEY.
      const previousAccessKey = process.env.MINIO_ACCESS_KEY;
      const previousSecretKey = process.env.MINIO_SECRET_KEY;
      const previousKeyFormat = process.env.ARGO_KEYFORMAT;
      process.env.MINIO_ACCESS_KEY = 'server-access-key';
      process.env.MINIO_SECRET_KEY = 'server-secret-key';
      process.env.ARGO_KEYFORMAT =
        'artifacts/{{workflow.namespace}}/{{workflow.name}}/{{workflow.creationTimestamp.Y}}/{{workflow.creationTimestamp.m}}/{{workflow.creationTimestamp.d}}/{{pod.name}}/';

      const objStream = new PassThrough();
      const mockedClient: Mock = MinioClient as any;
      MinioClient.prototype.getObject = vi.fn().mockResolvedValueOnce(objStream) as any;
      objStream.end('some fake logs.');

      try {
        await getPodLogsStreamFromWorkflow(
          'workflow-name-system-container-impl-abc',
          '2024-07-09',
          'my-user-namespace',
        );
      } finally {
        process.env.MINIO_ACCESS_KEY = previousAccessKey;
        process.env.MINIO_SECRET_KEY = previousSecretKey;
        if (previousKeyFormat === undefined) {
          delete process.env.ARGO_KEYFORMAT;
        } else {
          process.env.ARGO_KEYFORMAT = previousKeyFormat;
        }
      }

      expect(mockedGetK8sSecret).not.toBeCalled();
      // The client is built using the server's own environment credentials
      // rather than the customer-namespace Secret, so the workflow-status log
      // path works against the shared store instead of failing anonymously.
      expect(mockedClient).toBeCalledWith({
        accessKey: 'server-access-key',
        endPoint: 'seaweedfs.kubeflow',
        port: 9000,
        secretKey: 'server-secret-key',
        useSSL: false,
      });
    });

    it.each([
      {
        bucket: 'other-bucket',
        key: 'artifacts/my-user-namespace/workflow-name/2024/07/09/workflow-name-system-container-impl-abc/main.log',
        name: 'another bucket',
      },
      {
        bucket: 'mlpipeline',
        key: 'artifacts/my-user-namespace/other-workflow/2024/07/09/other-pod/main.log',
        name: "another workflow's key",
      },
    ])(
      'rejects workflow metadata pointing at $name with shared credentials',
      async ({ bucket, key }) => {
        const sampleWorkflow = {
          status: {
            artifactRepositoryRef: {
              artifactRepository: {
                archiveLogs: true,
                s3: {
                  bucket,
                  endpoint: 'seaweedfs.kubeflow:9000',
                  insecure: true,
                  key: 'unused',
                },
              },
            },
            nodes: {
              'workflow-name-abc': {
                outputs: { artifacts: [{ name: 'main-logs', s3: { key } }] },
              },
            },
          },
        };
        const mockedGetArgoWorkflow: Mock = getArgoWorkflow as any;
        mockedGetArgoWorkflow.mockResolvedValueOnce(sampleWorkflow);
        const mockedGetServerNamespace: Mock = getServerNamespace as any;
        mockedGetServerNamespace.mockReturnValue('kubeflow');
        const mockedClient: Mock = MinioClient as any;
        const previousKeyFormat = process.env.ARGO_KEYFORMAT;
        process.env.ARGO_KEYFORMAT =
          'artifacts/{{workflow.namespace}}/{{workflow.name}}/{{workflow.creationTimestamp.Y}}/{{workflow.creationTimestamp.m}}/{{workflow.creationTimestamp.d}}/{{pod.name}}';

        try {
          await expect(
            getPodLogsStreamFromWorkflow(
              'workflow-name-system-container-impl-abc',
              '2024-07-09',
              'my-user-namespace',
            ),
          ).rejects.toThrow('outside the configured namespace-scoped bucket and key');
        } finally {
          if (previousKeyFormat === undefined) {
            delete process.env.ARGO_KEYFORMAT;
          } else {
            process.env.ARGO_KEYFORMAT = previousKeyFormat;
          }
        }
        expect(mockedClient).not.toBeCalled();
      },
    );

    it('uses AWS credentials for a configured cross-namespace AWS archive endpoint', async () => {
      const sampleWorkflow = {
        status: {
          artifactRepositoryRef: {
            artifactRepository: {
              archiveLogs: true,
              s3: {
                bucket: 'mlpipeline',
                endpoint: 's3.example.test',
                insecure: false,
                key: 'unused',
              },
            },
          },
          nodes: {
            'workflow-name-abc': {
              outputs: {
                artifacts: [
                  {
                    name: 'main-logs',
                    s3: {
                      key: 'artifacts/my-user-namespace/workflow-name/2024/07/09/workflow-name-system-container-impl-abc/main.log',
                    },
                  },
                ],
              },
            },
          },
        },
      };
      const mockedGetArgoWorkflow: Mock = getArgoWorkflow as any;
      mockedGetArgoWorkflow.mockResolvedValueOnce(sampleWorkflow);
      const mockedGetServerNamespace: Mock = getServerNamespace as any;
      mockedGetServerNamespace.mockReturnValue('kubeflow');
      const mockedClient: Mock = MinioClient as any;
      const objStream = new PassThrough();
      MinioClient.prototype.getObject = vi.fn().mockResolvedValueOnce(objStream) as any;
      objStream.end('some fake logs.');

      const names = [
        'ARGO_KEYFORMAT',
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'AWS_S3_ENDPOINT',
      ] as const;
      const previous = Object.fromEntries(names.map((name) => [name, process.env[name]]));
      process.env.ARGO_KEYFORMAT =
        'artifacts/{{workflow.namespace}}/{{workflow.name}}/{{workflow.creationTimestamp.Y}}/{{workflow.creationTimestamp.m}}/{{workflow.creationTimestamp.d}}/{{pod.name}}';
      process.env.AWS_ACCESS_KEY_ID = 'aws-access-key';
      process.env.AWS_SECRET_ACCESS_KEY = 'aws-secret-key';
      process.env.AWS_S3_ENDPOINT = 's3.example.test';

      try {
        await getPodLogsStreamFromWorkflow(
          'workflow-name-system-container-impl-abc',
          '2024-07-09',
          'my-user-namespace',
        );
      } finally {
        for (const name of names) {
          const value = previous[name];
          if (value === undefined) {
            delete process.env[name];
          } else {
            process.env[name] = value;
          }
        }
      }

      expect(mockedClient).toBeCalledWith({
        accessKey: 'aws-access-key',
        endPoint: 's3.example.test',
        port: 443,
        secretKey: 'aws-secret-key',
        useSSL: true,
      });
    });

    it('reads the object-store Secret from the server namespace when the run namespace is omitted (standalone)', async () => {
      const sampleWorkflow = {
        apiVersion: 'argoproj.io/v1alpha1',
        kind: 'Workflow',
        status: {
          artifactRepositoryRef: {
            artifactRepository: {
              archiveLogs: true,
              s3: {
                accessKeySecret: { key: 'accessKey', name: 'accessKeyName' },
                bucket: 'bucket',
                endpoint: 'seaweedfs.kubeflow:9000',
                insecure: true,
                key: 'prefix/workflow-name/workflow-name-system-container-impl-abc/some-artifact.csv',
                secretKeySecret: { key: 'secretKey', name: 'secretKeyName' },
              },
            },
          },
          nodes: {
            'workflow-name-abc': {
              outputs: {
                artifacts: [
                  {
                    name: 'main-logs',
                    s3: {
                      key: 'prefix/workflow-name/workflow-name-system-container-impl-abc/main.log',
                    },
                  },
                ],
              },
            },
          },
        },
      };

      const mockedGetArgoWorkflow: Mock = getArgoWorkflow as any;
      mockedGetArgoWorkflow.mockResolvedValueOnce(sampleWorkflow);

      // Standalone mode omits the namespace; the run is effectively in the server
      // namespace, so the credential Secret is read from the server namespace.
      const mockedGetServerNamespace: Mock = getServerNamespace as any;
      mockedGetServerNamespace.mockReturnValue('kubeflow');

      const mockedGetK8sSecret: Mock = getK8sSecret as any;
      mockedGetK8sSecret.mockResolvedValue('custom-store-secret');

      const objStream = new PassThrough();
      const mockedClient: Mock = MinioClient as any;
      MinioClient.prototype.getObject = vi.fn().mockResolvedValueOnce(objStream) as any;
      objStream.end('some fake logs.');

      await getPodLogsStreamFromWorkflow(
        'workflow-name-system-container-impl-abc',
        '2024-07-09',
        undefined,
      );

      // The Secret is read from the server namespace, never a user namespace.
      expect(mockedGetK8sSecret).toBeCalledTimes(2);
      expect(mockedGetK8sSecret).toBeCalledWith('accessKeyName', 'accessKey', 'kubeflow');
      expect(mockedGetK8sSecret).toBeCalledWith('secretKeyName', 'secretKey', 'kubeflow');

      // The custom object-store credentials from the Secret are honored rather
      // than falling back to default env credentials.
      expect(mockedClient).toBeCalledWith({
        accessKey: 'custom-store-secret',
        endPoint: 'seaweedfs.kubeflow',
        port: 9000,
        secretKey: 'custom-store-secret',
        useSSL: false,
      });
    });

    it('falls back to env credentials for an omitted namespace when the artifact references no Secret', async () => {
      const sampleWorkflow = {
        apiVersion: 'argoproj.io/v1alpha1',
        kind: 'Workflow',
        status: {
          artifactRepositoryRef: {
            artifactRepository: {
              archiveLogs: true,
              s3: {
                // No accessKeySecret / secretKeySecret: the artifact repository
                // does not reference a credential Secret.
                bucket: 'bucket',
                endpoint: 'seaweedfs.kubeflow:9000',
                insecure: true,
                key: 'prefix/workflow-name/workflow-name-system-container-impl-abc/some-artifact.csv',
              },
            },
          },
          nodes: {
            'workflow-name-abc': {
              outputs: {
                artifacts: [
                  {
                    name: 'main-logs',
                    s3: {
                      key: 'prefix/workflow-name/workflow-name-system-container-impl-abc/main.log',
                    },
                  },
                ],
              },
            },
          },
        },
      };

      const mockedGetArgoWorkflow: Mock = getArgoWorkflow as any;
      mockedGetArgoWorkflow.mockResolvedValueOnce(sampleWorkflow);

      const mockedGetServerNamespace: Mock = getServerNamespace as any;
      mockedGetServerNamespace.mockReturnValue('kubeflow');

      const mockedGetK8sSecret: Mock = getK8sSecret as any;

      const previousAccessKey = process.env.MINIO_ACCESS_KEY;
      const previousSecretKey = process.env.MINIO_SECRET_KEY;
      process.env.MINIO_ACCESS_KEY = 'server-access-key';
      process.env.MINIO_SECRET_KEY = 'server-secret-key';

      const objStream = new PassThrough();
      const mockedClient: Mock = MinioClient as any;
      MinioClient.prototype.getObject = vi.fn().mockResolvedValueOnce(objStream) as any;
      objStream.end('some fake logs.');

      try {
        await getPodLogsStreamFromWorkflow(
          'workflow-name-system-container-impl-abc',
          '2024-07-09',
          undefined,
        );
      } finally {
        process.env.MINIO_ACCESS_KEY = previousAccessKey;
        process.env.MINIO_SECRET_KEY = previousSecretKey;
      }

      // With no Secret referenced, no Secret read is attempted and the frontend's
      // own configured env credentials are used.
      expect(mockedGetK8sSecret).not.toBeCalled();
      expect(mockedClient).toBeCalledWith({
        accessKey: 'server-access-key',
        endPoint: 'seaweedfs.kubeflow',
        port: 9000,
        secretKey: 'server-secret-key',
        useSSL: false,
      });
    });

    it.each(['^.*$', '.*', '^.+$', '^[\\s\\S]*$'])(
      'rejects an unconfigured workflow-status endpoint despite broad domain regex %s (SSRF)',
      async (allowedDomain) => {
        // The endpoint is resolved from the Argo Workflow status, which a
        // namespace-owning tenant can influence via a namespace-local
        // artifact-repositories configmap. The legacy allow-all regex must not let
        // the shared server build a client toward an attacker-chosen host.
        const sampleWorkflow = {
          apiVersion: 'argoproj.io/v1alpha1',
          kind: 'Workflow',
          status: {
            artifactRepositoryRef: {
              artifactRepository: {
                archiveLogs: true,
                s3: {
                  bucket: 'bucket',
                  endpoint: 'attacker.evil.example:9000',
                  insecure: true,
                  key: 'prefix/workflow-name/workflow-name-system-container-impl-abc/some-artifact.csv',
                },
              },
            },
            nodes: {
              'workflow-name-abc': {
                outputs: {
                  artifacts: [
                    {
                      name: 'main-logs',
                      s3: {
                        key: 'prefix/workflow-name/workflow-name-system-container-impl-abc/main.log',
                      },
                    },
                  ],
                },
              },
            },
          },
        };

        const mockedGetArgoWorkflow: Mock = getArgoWorkflow as any;
        mockedGetArgoWorkflow.mockResolvedValueOnce(sampleWorkflow);

        const mockedGetServerNamespace: Mock = getServerNamespace as any;
        mockedGetServerNamespace.mockReturnValue('kubeflow');

        const mockedClient: Mock = MinioClient as any;
        mockedClient.mockClear();
        MinioClient.prototype.getObject = vi.fn() as any;

        const previousAllowedDomain = process.env.ALLOWED_ARTIFACT_DOMAIN_REGEX;
        process.env.ALLOWED_ARTIFACT_DOMAIN_REGEX = allowedDomain;

        try {
          await expect(
            getPodLogsStreamFromWorkflow(
              'workflow-name-system-container-impl-abc',
              '2024-07-09',
              'kubeflow',
            ),
          ).rejects.toThrow('not in the allowed artifact domain list');
        } finally {
          if (previousAllowedDomain === undefined) {
            delete process.env.ALLOWED_ARTIFACT_DOMAIN_REGEX;
          } else {
            process.env.ALLOWED_ARTIFACT_DOMAIN_REGEX = previousAllowedDomain;
          }
        }

        // No object-store client is constructed and no outbound object read is made.
        expect(mockedClient).not.toBeCalled();
      },
    );

    it('allows a workflow-status endpoint that exactly matches server configuration', async () => {
      const sampleWorkflow = {
        apiVersion: 'argoproj.io/v1alpha1',
        kind: 'Workflow',
        status: {
          artifactRepositoryRef: {
            artifactRepository: {
              archiveLogs: true,
              s3: {
                bucket: 'bucket',
                endpoint: 'seaweedfs.kubeflow:9000',
                insecure: true,
                key: 'prefix/workflow-name/workflow-name-system-container-impl-abc/some-artifact.csv',
              },
            },
          },
          nodes: {
            'workflow-name-abc': {
              outputs: {
                artifacts: [
                  {
                    name: 'main-logs',
                    s3: {
                      key: 'prefix/workflow-name/workflow-name-system-container-impl-abc/main.log',
                    },
                  },
                ],
              },
            },
          },
        },
      };

      const mockedGetArgoWorkflow: Mock = getArgoWorkflow as any;
      mockedGetArgoWorkflow.mockResolvedValueOnce(sampleWorkflow);

      const mockedGetServerNamespace: Mock = getServerNamespace as any;
      mockedGetServerNamespace.mockReturnValue('kubeflow');

      const mockedGetK8sSecret: Mock = getK8sSecret as any;
      mockedGetK8sSecret.mockResolvedValue('someSecret');

      const objStream = new PassThrough();
      const mockedClient: Mock = MinioClient as any;
      mockedClient.mockClear();
      MinioClient.prototype.getObject = vi.fn().mockResolvedValueOnce(objStream) as any;
      objStream.end('some fake logs.');

      await getPodLogsStreamFromWorkflow(
        'workflow-name-system-container-impl-abc',
        '2024-07-09',
        'kubeflow',
      );

      // The configured endpoint is accepted and the object-store client is
      // constructed toward it (the outbound read then proceeds as normal).
      expect(mockedClient).toBeCalledTimes(1);
      expect(mockedClient).toBeCalledWith(
        expect.objectContaining({ endPoint: 'seaweedfs.kubeflow', port: 9000, useSSL: false }),
      );
      const clientInstance = mockedClient.mock.results[0].value;
      expect(clientInstance.getObject).toBeCalledWith(
        'bucket',
        'prefix/workflow-name/workflow-name-system-container-impl-abc/main.log',
      );
    });
  });
});
