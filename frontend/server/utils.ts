// Copyright 2018-2020 The Kubeflow Authors
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
import { readFileSync, promises as fsPromises } from 'fs';
import type { FileHandle } from 'fs/promises';
import { Transform, TransformOptions } from 'stream';
import { posix as path } from 'path';

/** get the server address from host, port, and schema (defaults to 'http'). */
export function getAddress({
  host,
  port,
  namespace,
  schema,
}: {
  host: string;
  port?: string | number;
  namespace?: string;
  schema?: string;
}) {
  namespace = namespace ? `.${namespace}` : '';
  port = port ? `:${port}` : '';
  return `${schema}://${host}${namespace}${port}`;
}

export function equalArrays(a1: any[], a2: any[]): boolean {
  if (!Array.isArray(a1) || !Array.isArray(a2) || a1.length !== a2.length) {
    return false;
  }
  return JSON.stringify(a1) === JSON.stringify(a2);
}

export function generateRandomString(length: number): string {
  let d = new Date().getTime();
  function randomChar(): string {
    const r = Math.trunc((d + Math.random() * 16) % 16);
    d = Math.floor(d / 16);
    return r.toString(16);
  }
  let str = '';
  for (let i = 0; i < length; ++i) {
    str += randomChar();
  }
  return str;
}

export function loadJSON<T>(filepath?: string, defaultValue?: T): T | undefined {
  if (!filepath) {
    return defaultValue;
  }
  try {
    return JSON.parse(readFileSync(filepath, 'utf-8'));
  } catch (error) {
    console.error(`Failed reading json data from '${filepath}':`);
    console.error(error);
    return defaultValue;
  }
}

export function parseJSONString<T>(str: string) {
  try {
    const jsonValue: T = JSON.parse(str);
    return jsonValue;
  } catch (e) {
    return undefined;
  }
}

/** Strip trailing slashes so `pipeline1/` and `pipeline1` compare equal. */
function trimTrailingSlashes(subPath: string): string {
  return subPath.replace(/\/+$/, '');
}

/**
 * Whether an already-resolved absolute path stays within a resolved mount root
 * (equal to it or a descendant). `resolvedMount` may itself be `/`, whose
 * trailing slash must not be doubled or every descendant would be rejected.
 */
function isPathInsideMount(resolvedPath: string, resolvedMount: string): boolean {
  const mountPrefix = resolvedMount.endsWith('/') ? resolvedMount : `${resolvedMount}/`;
  return resolvedPath === resolvedMount || resolvedPath.startsWith(mountPrefix);
}

/**
 * Whether `filePathInVolume` lies within `subPath`, matching only on path
 * segment boundaries. A plain `startsWith` check would treat siblings such as
 * `pipeline10/file.txt` as being inside `pipeline1`; requiring an exact match
 * or a trailing `/` avoids that false positive. Trailing slashes on `subPath`
 * are ignored so a configured `pipeline1/` still matches `pipeline1/file.txt`.
 */
function isWithinSubPath(filePathInVolume: string, subPath: string): boolean {
  const normalizedSubPath = trimTrailingSlashes(subPath);
  return (
    filePathInVolume === normalizedSubPath || filePathInVolume.startsWith(`${normalizedSubPath}/`)
  );
}

/**
 * find final file path in pod:
 * 1. check volume and volume mount exist in pod
 * 2. if volume mount configured with subPath, check filePathInVolume is within subPath and prune filePathInVolume
 * 3. concat volume mount path with pruned filePathInVolume as final path or error message if check failed
 * @param pod contains volumes and volume mounts info
 * @param options
 *  - containerNames optional, will match to find container or container[0] in pod will be used
 *  - volumeMountName container volume mount name
 *  - filePathInVolume file path in volume
 * @return [final file path, error message if check failed, resolved volume mount path].
 *   The mount path lets callers re-validate the real (symlink-resolved) path
 *   before reading; it is '' when an error is returned.
 */
export function findFileOnPodVolume(
  pod: any,
  options: {
    containerNames: string[] | undefined;
    volumeMountName: string;
    filePathInVolume: string;
  },
): [string, string | undefined, string] {
  const { containerNames, volumeMountName, filePathInVolume } = options;

  const volumes = pod?.spec?.volumes;
  const prefixErrorMessage = `Cannot find file "volume://${volumeMountName}/${filePathInVolume}" in pod "${
    pod?.metadata?.name || 'unknown'
  }":`;
  // volumes not specified or volume named ${volumeMountName} not specified
  if (!Array.isArray(volumes) || !volumes.find((v) => v?.name === volumeMountName)) {
    return ['', `${prefixErrorMessage} volume "${volumeMountName}" not configured`, ''];
  }

  // get pod main container
  let container;
  if (Array.isArray(pod.spec.containers)) {
    if (containerNames) {
      // find main container by container name match containerNames
      container = pod.spec.containers.find((c: { [name: string]: string }) =>
        containerNames.includes(c.name),
      );
    } else {
      // use containers[0] as pod main container
      container = pod.spec.containers[0];
    }
  }

  if (!container) {
    const containerNamesMessage = containerNames ? containerNames.join('" or "') : '';
    return ['', `${prefixErrorMessage} container "${containerNamesMessage}" not found`, ''];
  }

  const volumeMounts = container.volumeMounts;
  if (!Array.isArray(volumeMounts)) {
    return ['', `${prefixErrorMessage} volume "${volumeMountName}" not mounted`, ''];
  }

  // find volumes mount
  const volumeMount = volumeMounts.find((v) => {
    // volume name must be same
    if (v?.name !== volumeMountName) {
      return false;
    }
    // if volume subPath set, filePathInVolume must be within subPath
    if (v?.subPath) {
      return isWithinSubPath(filePathInVolume, v.subPath);
    }
    return true;
  });

  if (!volumeMount) {
    return [
      '',
      `${prefixErrorMessage} volume "${volumeMountName}" not mounted or volume "${volumeMountName}" with subPath (which is prefix of ${filePathInVolume}) not mounted`,
      '',
    ];
  }

  // resolve file path
  const [filePath, err] = resolveFilePathOnVolume({
    filePathInVolume,
    volumeMountPath: volumeMount.mountPath,
    volumeMountSubPath: volumeMount.subPath,
  });

  if (err) {
    return ['', `${prefixErrorMessage} ${err}`, ''];
  }
  return [filePath, undefined, volumeMount.mountPath];
}

export function resolveFilePathOnVolume(volume: {
  filePathInVolume: string;
  volumeMountPath: string;
  volumeMountSubPath: string | undefined;
}): [string, string | undefined] {
  const { filePathInVolume, volumeMountPath, volumeMountSubPath } = volume;
  let joined: string;
  if (!volumeMountSubPath) {
    joined = path.join(volumeMountPath, filePathInVolume);
  } else if (isWithinSubPath(filePathInVolume, volumeMountSubPath)) {
    // Prune the subPath prefix using its trailing-slash-trimmed length so the
    // remainder lines up with the boundary `isWithinSubPath` matched on.
    const normalizedSubPath = trimTrailingSlashes(volumeMountSubPath);
    joined = path.join(volumeMountPath, filePathInVolume.substring(normalizedSubPath.length));
  } else {
    return [
      '',
      `File ${filePathInVolume} not mounted, expecting the file to be inside volume mount subpath ${volumeMountSubPath}`,
    ];
  }
  // Reject any path that escapes the mount point via `..` segments or
  // absolute components — path.join collapses `..` so a `filePathInVolume`
  // of `../../etc/passwd` would otherwise resolve outside the volume.
  const normalizedMount = path.resolve(volumeMountPath);
  const normalizedJoined = path.resolve(joined);
  if (!isPathInsideMount(normalizedJoined, normalizedMount)) {
    return ['', `File ${filePathInVolume} escapes volume mount ${volumeMountPath}`];
  }
  return [normalizedJoined, undefined];
}

/**
 * Open `filePath` and confirm the file that was actually opened resolves to a
 * location within `volumeMountPath`, returning the open handle to stream from.
 *
 * The lexical check in `resolveFilePathOnVolume` blocks `..` escapes but not a
 * symlink inside the volume pointing outside it. Validating a resolved *path*
 * and then re-opening it by name would still be raceable: an attacker who can
 * write the volume could swap the file for a symlink between the check and the
 * open, and `stat`/`createReadStream` follow symlinks. Validating the opened
 * file descriptor instead closes that time-of-check/time-of-use gap — the read
 * streams from this same handle, never re-resolving the name. On Linux the real
 * target of the open descriptor is read from `/proc/self/fd/<fd>`; where that is
 * unavailable the path is resolved directly as a fallback (ml-pipeline-ui runs
 * on Linux, where the fd path is used).
 *
 * Errors from opening (for example a missing file) propagate to the caller so
 * existing not-found handling is preserved. On success the caller owns the
 * returned handle and must close it (streaming from it closes it on completion).
 * @return [open file handle to read from, error message if the opened file escapes the mount]
 */
export async function openRealFileWithinMount(
  filePath: string,
  volumeMountPath: string,
): Promise<[FileHandle | undefined, string | undefined]> {
  const fileHandle = await fsPromises.open(filePath, 'r');
  try {
    const realMountPath = await fsPromises.realpath(volumeMountPath);
    let realOpenedPath: string;
    try {
      realOpenedPath = await fsPromises.realpath(`/proc/self/fd/${fileHandle.fd}`);
    } catch {
      realOpenedPath = await fsPromises.realpath(filePath);
    }
    if (!isPathInsideMount(realOpenedPath, realMountPath)) {
      await fileHandle.close();
      return [undefined, `File ${filePath} escapes volume mount ${volumeMountPath} via symlink`];
    }
    return [fileHandle, undefined];
  } catch (err) {
    await fileHandle.close();
    throw err;
  }
}

export interface PreviewStreamOptions extends TransformOptions {
  peek: number;
}

/**
 * Transform stream that only stream the first X number of bytes.
 */
export class PreviewStream extends Transform {
  constructor({ peek, ...opts }: PreviewStreamOptions) {
    // acts like passthrough
    let transform: TransformOptions['transform'] = (chunk, _encoding, callback) =>
      callback(undefined, chunk);
    // implements preview - peek must be positive number
    if (peek && peek > 0) {
      let size = 0;
      transform = (chunk, _encoding, callback) => {
        const delta = peek - size;
        size += chunk.length;
        if (size >= peek) {
          callback(undefined, chunk.slice(0, delta));
          this.resume(); // do not handle any subsequent data
          return;
        }
        callback(undefined, chunk);
      };
    }
    super({ ...opts, transform });
  }
}

export interface ErrorDetails {
  message: string;
  additionalInfo: any;
}
const UNKOWN_ERROR = 'Unknown error';
export async function parseError(error: any): Promise<ErrorDetails> {
  return (
    parseK8sError(error) ||
    (await parseKfpApiError(error)) ||
    parseGenericError(error) || { message: UNKOWN_ERROR, additionalInfo: error }
  );
}

function parseGenericError(error: any): ErrorDetails | undefined {
  if (!error) {
    return undefined;
  } else if (typeof error === 'string') {
    return {
      message: error,
      additionalInfo: error,
    };
  } else if (error instanceof Error) {
    return { message: error.message, additionalInfo: error };
  } else if (error.message && typeof error.message === 'string') {
    return { message: error.message, additionalInfo: error };
  } else if (
    error.url &&
    typeof error.url === 'string' &&
    error.status &&
    typeof error.status === 'number' &&
    error.statusText &&
    typeof error.statusText === 'string'
  ) {
    const { url, status, statusText } = error;
    return {
      message: `Fetching ${url} failed with status code ${status} and message: ${statusText}`,
      additionalInfo: { url, status, statusText },
    };
  }
  // Cannot understand error type
  return undefined;
}
async function parseKfpApiError(error: any): Promise<ErrorDetails | undefined> {
  if (!error) {
    return undefined;
  }

  // Swagger client throws the fetch response directly (with json()).
  // OpenAPI client throws ResponseError with response at error.response.
  const response =
    error && typeof error.json === 'function'
      ? error
      : error.response && typeof error.response.json === 'function'
        ? error.response
        : undefined;

  if (!response) {
    return undefined;
  }

  const canClone = typeof response.clone === 'function';

  if (!canClone && typeof response.text === 'function') {
    try {
      // Without clone(), read the body once as text so we can recover both JSON
      // and plain-text error payloads from the same buffer.
      const text = await response.text();
      const parsed = parseJSONString<{ error?: string; details?: any }>(text);
      if (parsed && typeof parsed.error === 'string') {
        return { message: parsed.error, additionalInfo: parsed.details ?? parsed };
      }
      if (text) {
        return { message: text, additionalInfo: text };
      }
      return undefined;
    } catch (_err) {
      return undefined;
    }
  }

  try {
    const jsonSource = canClone ? response.clone() : response;
    const json = await jsonSource.json();
    const { error: message, details } = json;
    if (typeof message === 'string') {
      return {
        message,
        additionalInfo: details ?? json,
      };
    }
  } catch (_err) {
    // Fall through and try parsing response text.
  }

  try {
    const textSource = canClone ? response.clone() : response;
    const text = await textSource.text();
    const parsed = parseJSONString<{ error?: string; details?: any }>(text);
    if (parsed && typeof parsed.error === 'string') {
      return { message: parsed.error, additionalInfo: parsed.details ?? parsed };
    }
    if (text) {
      return { message: text, additionalInfo: text };
    }
  } catch (_err) {
    return undefined;
  }

  return undefined;
}
function parseK8sError(error: any): ErrorDetails | undefined {
  if (!error || !error.body || typeof error.body !== 'object') {
    return undefined;
  }

  if (typeof error.body.message !== 'string') {
    return undefined;
  }

  // Kubernetes client http error has body with all the info.
  // Example error.body
  // {
  //   kind: 'Status',
  //   apiVersion: 'v1',
  //   metadata: {},
  //   status: 'Failure',
  //   message: 'pods "test-pod" not found',
  //   reason: 'NotFound',
  //   details: { name: 'test-pod', kind: 'pods' },
  //   code: 404
  // }
  return {
    message: error.body.message,
    additionalInfo: error.body,
  };
}

export function isAllowedResourceName(name: string): boolean {
  return name.length > 0 && name.length <= 63 && /^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/.test(name);
}
