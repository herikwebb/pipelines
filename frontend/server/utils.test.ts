// Copyright 2020 The Kubeflow Authors
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
import { PassThrough } from 'stream';
import { promises as fsPromises } from 'fs';
import * as os from 'os';
import * as nodePath from 'path';
import {
  PreviewStream,
  findFileOnPodVolume,
  parseError,
  resolveFilePathOnVolume,
  openRealFileWithinMount,
} from './utils.js';

describe('utils', () => {
  describe('PreviewStream', () => {
    it('should stream first 5 bytes', async () => {
      const peek = 5;
      const input = 'some string that will be truncated.';
      const source = new PassThrough();
      const preview = new PreviewStream({ peek });
      await new Promise<void>((resolve) => {
        const dst = source.pipe(preview).on('end', resolve);
        source.end(input);
        dst.once('readable', () => expect(dst.read().toString()).toBe(input.slice(0, peek)));
      });
    });

    it('should stream everything if peek==0', async () => {
      const peek = 0;
      const input = 'some string that will be truncated.';
      const source = new PassThrough();
      const preview = new PreviewStream({ peek });
      await new Promise<void>((resolve) => {
        const dst = source.pipe(preview).on('end', resolve);
        source.end(input);
        dst.once('readable', () => expect(dst.read().toString()).toBe(input));
      });
    });
  });

  describe('findFileOnPodVolume', () => {
    const podTemplateSpec = {
      spec: {
        containers: [
          {
            volumeMounts: [
              {
                name: 'output',
                mountPath: '/main',
              },
              {
                name: 'artifact',
                subPath: 'pipeline1',
                mountPath: '/main1',
              },
              {
                name: 'artifact',
                subPath: 'pipeline2',
                mountPath: '/main2',
              },
            ],
            name: 'main',
          },
          {
            volumeMounts: [
              {
                name: 'output',
                mountPath: '/data',
              },
              {
                name: 'artifact',
                subPath: 'pipeline1',
                mountPath: '/data1',
              },
              {
                name: 'artifact',
                subPath: 'pipeline2',
                mountPath: '/data2',
              },
            ],
            name: 'ml-pipeline-ui',
          },
        ],
        volumes: [
          {
            name: 'output',
            hostPath: {
              path: '/data/output',
              type: 'Directory',
            },
          },
          {
            name: 'artifact',
            persistentVolumeClaim: {
              claimName: 'artifact_pvc',
            },
          },
        ],
      },
    };

    it('parse file path with containerNames', () => {
      const [filePath, err] = findFileOnPodVolume(podTemplateSpec, {
        containerNames: ['ml-pipeline-ui', 'ml-pipeline-ui-artifact'],
        volumeMountName: 'output',
        filePathInVolume: 'a/b/c',
      });
      expect(err).toEqual(undefined);
      expect(filePath).toEqual('/data/a/b/c');
    });

    it('parse file path with containerNames and subPath', () => {
      const [filePath, err] = findFileOnPodVolume(podTemplateSpec, {
        containerNames: ['ml-pipeline-ui', 'ml-pipeline-ui-artifact'],
        volumeMountName: 'artifact',
        filePathInVolume: 'pipeline1/a/b/c',
      });
      expect(err).toEqual(undefined);
      expect(filePath).toEqual('/data1/a/b/c');
    });

    it('parse file path without containerNames', () => {
      const [filePath, err] = findFileOnPodVolume(podTemplateSpec, {
        containerNames: undefined,
        volumeMountName: 'output',
        filePathInVolume: 'a/b/c',
      });
      expect(err).toEqual(undefined);
      expect(filePath).toEqual('/main/a/b/c');
    });

    it('parse file path error with not exist volume', () => {
      const [filePath, err] = findFileOnPodVolume(podTemplateSpec, {
        containerNames: undefined,
        volumeMountName: 'other',
        filePathInVolume: 'a/b/c',
      });
      expect(err).toEqual(
        'Cannot find file "volume://other/a/b/c" in pod "unknown": volume "other" not configured',
      );
      expect(filePath).toEqual('');
    });

    it('parse file path error with not exist container', () => {
      const [filePath, err] = findFileOnPodVolume(podTemplateSpec, {
        containerNames: ['other1', 'other2'],
        volumeMountName: 'output',
        filePathInVolume: 'a/b/c',
      });
      expect(err).toEqual(
        'Cannot find file "volume://output/a/b/c" in pod "unknown": container "other1" or "other2" not found',
      );
      expect(filePath).toEqual('');
    });

    it('parse file path error with volume not mount error', () => {
      const [filePath, err] = findFileOnPodVolume(podTemplateSpec, {
        containerNames: undefined,
        volumeMountName: 'artifact',
        filePathInVolume: 'a/b/c',
      });
      expect(err).toEqual(
        'Cannot find file "volume://artifact/a/b/c" in pod "unknown": volume "artifact" not mounted or volume "artifact" with subPath (which is prefix of a/b/c) not mounted',
      );
      expect(filePath).toEqual('');
    });

    it('does not match a subPath that is only a sibling prefix', () => {
      // `pipeline10` shares a textual prefix with the mounted subPath
      // `pipeline1` but is a distinct sibling directory, so it must not
      // resolve against the `pipeline1` mount.
      const [filePath, err] = findFileOnPodVolume(podTemplateSpec, {
        containerNames: undefined,
        volumeMountName: 'artifact',
        filePathInVolume: 'pipeline10/a/b/c',
      });
      expect(err).toEqual(
        'Cannot find file "volume://artifact/pipeline10/a/b/c" in pod "unknown": volume "artifact" not mounted or volume "artifact" with subPath (which is prefix of pipeline10/a/b/c) not mounted',
      );
      expect(filePath).toEqual('');
    });

    it('propagates the resolver error when a matched path escapes the mount', () => {
      const [filePath, err] = findFileOnPodVolume(podTemplateSpec, {
        containerNames: undefined,
        volumeMountName: 'artifact',
        filePathInVolume: 'pipeline1/../../etc/passwd',
      });
      expect(err).toEqual(
        'Cannot find file "volume://artifact/pipeline1/../../etc/passwd" in pod "unknown": File pipeline1/../../etc/passwd escapes volume mount /main1',
      );
      expect(filePath).toEqual('');
    });
  });

  describe('resolveFilePathOnVolume', () => {
    it('undefined volumeMountSubPath', () => {
      const path = resolveFilePathOnVolume({
        filePathInVolume: 'a/b/c',
        volumeMountPath: '/data',
        volumeMountSubPath: undefined,
      });
      expect(path).toEqual(['/data/a/b/c', undefined]);
    });

    it('with volumeMountSubPath', () => {
      const path = resolveFilePathOnVolume({
        volumeMountPath: '/data',
        filePathInVolume: 'a/b/c',
        volumeMountSubPath: 'a',
      });
      expect(path).toEqual(['/data/b/c', undefined]);
    });

    it('with multiple layer volumeMountSubPath', () => {
      const path = resolveFilePathOnVolume({
        volumeMountPath: '/data',
        filePathInVolume: 'a/b/c',
        volumeMountSubPath: 'a/b',
      });
      expect(path).toEqual(['/data/c', undefined]);
    });

    it('with not exist volumeMountSubPath', () => {
      const path = resolveFilePathOnVolume({
        volumeMountPath: '/data',
        filePathInVolume: 'a/b/c',
        volumeMountSubPath: 'other',
      });
      expect(path).toEqual([
        '',
        'File a/b/c not mounted, expecting the file to be inside volume mount subpath other',
      ]);
    });

    it('does not treat a sibling prefix as inside the subPath', () => {
      const path = resolveFilePathOnVolume({
        volumeMountPath: '/data',
        filePathInVolume: 'pipeline10/file.txt',
        volumeMountSubPath: 'pipeline1',
      });
      expect(path).toEqual([
        '',
        'File pipeline10/file.txt not mounted, expecting the file to be inside volume mount subpath pipeline1',
      ]);
    });

    it('rejects traversal that escapes the mount when no subPath is set', () => {
      const path = resolveFilePathOnVolume({
        volumeMountPath: '/etc/config',
        filePathInVolume: '../../var/run/secrets/kubernetes.io/serviceaccount/token',
        volumeMountSubPath: undefined,
      });
      expect(path).toEqual([
        '',
        'File ../../var/run/secrets/kubernetes.io/serviceaccount/token escapes volume mount /etc/config',
      ]);
    });

    it('rejects traversal that escapes the mount after subPath strip', () => {
      const path = resolveFilePathOnVolume({
        volumeMountPath: '/data1',
        filePathInVolume: 'pipeline1/../../etc/passwd',
        volumeMountSubPath: 'pipeline1',
      });
      expect(path).toEqual(['', 'File pipeline1/../../etc/passwd escapes volume mount /data1']);
    });

    it('allows descendant paths for a volume mounted at root', () => {
      const path = resolveFilePathOnVolume({
        volumeMountPath: '/',
        filePathInVolume: 'foo/bar',
        volumeMountSubPath: undefined,
      });
      expect(path).toEqual(['/foo/bar', undefined]);
    });

    it('matches a subPath configured with a trailing slash', () => {
      const path = resolveFilePathOnVolume({
        volumeMountPath: '/data',
        filePathInVolume: 'pipeline1/file.txt',
        volumeMountSubPath: 'pipeline1/',
      });
      expect(path).toEqual(['/data/file.txt', undefined]);
    });
  });

  describe('openRealFileWithinMount', () => {
    let mountDir: string;
    let outsideDir: string;

    beforeAll(async () => {
      mountDir = await fsPromises.mkdtemp(nodePath.join(os.tmpdir(), 'kfp-mount-'));
      outsideDir = await fsPromises.mkdtemp(nodePath.join(os.tmpdir(), 'kfp-outside-'));
      await fsPromises.writeFile(nodePath.join(mountDir, 'inside.txt'), 'ok');
      await fsPromises.writeFile(nodePath.join(outsideDir, 'secret.txt'), 'secret');
      // A symlink inside the mount that points to a file outside it.
      await fsPromises.symlink(
        nodePath.join(outsideDir, 'secret.txt'),
        nodePath.join(mountDir, 'escape'),
      );
    });

    afterAll(async () => {
      await fsPromises.rm(mountDir, { recursive: true, force: true });
      await fsPromises.rm(outsideDir, { recursive: true, force: true });
    });

    it('returns an open handle to a file inside the mount', async () => {
      const filePath = nodePath.join(mountDir, 'inside.txt');
      const [fileHandle, err] = await openRealFileWithinMount(filePath, mountDir);
      expect(err).toBeUndefined();
      expect(fileHandle).toBeDefined();
      const contents = await fileHandle!.readFile();
      expect(contents.toString()).toEqual('ok');
      await fileHandle!.close();
    });

    it('rejects (and does not return a handle for) a symlink escaping the mount', async () => {
      const filePath = nodePath.join(mountDir, 'escape');
      const [fileHandle, err] = await openRealFileWithinMount(filePath, mountDir);
      expect(fileHandle).toBeUndefined();
      expect(err).toContain('via symlink');
    });

    it('propagates the open error for a missing file', async () => {
      const filePath = nodePath.join(mountDir, 'does-not-exist');
      await expect(openRealFileWithinMount(filePath, mountDir)).rejects.toThrow();
    });
  });

  describe('parseError', () => {
    it('parses nested non-cloneable KFP error responses from text without consuming the body twice', async () => {
      let consumed = false;
      const response = {
        async json() {
          consumed = true;
          throw new Error('json parse failed');
        },
        async text() {
          if (consumed) {
            throw new Error('body already consumed');
          }
          consumed = true;
          return JSON.stringify({ error: 'backend exploded', details: { status: 500 } });
        },
      };

      await expect(parseError({ response })).resolves.toEqual({
        message: 'backend exploded',
        additionalInfo: { status: 500 },
      });
    });

    it('returns plain text for direct non-cloneable KFP responses', async () => {
      let consumed = false;
      const response = {
        async json() {
          consumed = true;
          throw new Error('json parse failed');
        },
        async text() {
          if (consumed) {
            throw new Error('body already consumed');
          }
          consumed = true;
          return 'plain text failure';
        },
      };

      await expect(parseError(response)).resolves.toEqual({
        message: 'plain text failure',
        additionalInfo: 'plain text failure',
      });
    });
  });
});
