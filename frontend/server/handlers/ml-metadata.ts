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

import { Handler } from 'express';
import {
  AuthorizeRequestResources,
  AuthorizeRequestVerb,
} from '../src/generated/apis/auth/index.js';
import { AuthorizeFn } from '../helpers/auth.js';
import { isAllowedResourceName } from '../utils.js';

/** Header carrying the namespace that scopes ML Metadata reads in multi-user mode. */
export const ML_METADATA_NAMESPACE_HEADER = 'x-kfp-namespace';

/** gRPC-web paths that mutate the shared ML Metadata store. */
const ML_METADATA_MUTATING_RPC_PATTERN =
  /\/ml_metadata\.MetadataStoreService\/(Put|Delete)[A-Za-z]*/;

/** gRPC-web paths that list all records without caller-provided scope. */
const ML_METADATA_UNSCOPED_LIST_RPCS = new Set([
  '/ml_metadata.MetadataStoreService/GetArtifacts',
  '/ml_metadata.MetadataStoreService/GetExecutions',
  '/ml_metadata.MetadataStoreService/GetContexts',
]);

export function isMlMetadataMutatingRpc(requestPath: string): boolean {
  return ML_METADATA_MUTATING_RPC_PATTERN.test(requestPath);
}

export function isMlMetadataUnscopedListRpc(requestPath: string): boolean {
  return ML_METADATA_UNSCOPED_LIST_RPCS.has(requestPath);
}

export function getMlMetadataAuthMiddleware(
  authorizeFn: AuthorizeFn,
  authEnabled: boolean,
  kubeflowUserIdHeader: string,
): Handler {
  return async (request, response, next) => {
    const requestPath = request.path;

    if (isMlMetadataMutatingRpc(requestPath)) {
      console.warn(
        `[SECURITY] Blocked ML Metadata write attempt. Path: ${request.originalUrl}`,
      );
      response.status(403).send('ML Metadata write operations are not allowed through the UI');
      return;
    }

    if (!authEnabled) {
      return next();
    }

    if (isMlMetadataUnscopedListRpc(requestPath)) {
      console.warn(
        `[SECURITY] Blocked unscoped ML Metadata list attempt. Path: ${request.originalUrl}`,
      );
      response
        .status(403)
        .send('Unscoped ML Metadata list operations are not allowed through the UI');
      return;
    }

    const userId = request.headers[kubeflowUserIdHeader.toLowerCase()];
    if (!userId) {
      console.warn(
        `[SECURITY] Unauthenticated ML Metadata access attempt. Path: ${request.originalUrl}`,
      );
      response.status(401).send('Authentication required for ML Metadata access');
      return;
    }

    const rawNamespace = request.headers[ML_METADATA_NAMESPACE_HEADER];
    const namespace: string | undefined = Array.isArray(rawNamespace)
      ? String(rawNamespace[0])
      : typeof rawNamespace === 'string'
        ? rawNamespace
        : undefined;

    if (!namespace) {
      console.warn(
        `[SECURITY] Missing ${ML_METADATA_NAMESPACE_HEADER} header. ` +
          `User: ${userId}, Path: ${request.originalUrl}`,
      );
      response
        .status(400)
        .send(`${ML_METADATA_NAMESPACE_HEADER} header is required when authentication is enabled`);
      return;
    }

    if (!isAllowedResourceName(namespace)) {
      console.warn(
        `[SECURITY] Invalid namespace format for ML Metadata access. ` +
          `User: ${userId}, Namespace: ${namespace}, Path: ${request.originalUrl}`,
      );
      response.status(400).send('Invalid namespace format');
      return;
    }

    const authError = await authorizeFn(
      {
        verb: AuthorizeRequestVerb.GET,
        resources: AuthorizeRequestResources.VIEWERS,
        namespace,
      },
      request,
    );

    if (authError) {
      console.warn(
        `[SECURITY] Unauthorized ML Metadata access attempt. ` +
          `User: ${userId}, Namespace: ${namespace}, Path: ${request.originalUrl}, ` +
          `Reason: ${authError.message}`,
      );
      response.status(403).send(authError.message);
      return;
    }

    next();
  };
}
