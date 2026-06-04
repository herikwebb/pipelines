// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law and agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { Handler, NextFunction, Request, Response } from 'express';
import { AuthorizeFn } from '../helpers/auth.js';
import {
  AuthorizeRequestResources,
  AuthorizeRequestVerb,
} from '../src/generated/apis/auth/index.js';
import { isAllowedResourceName } from '../utils.js';

/** MLMD custom property used to scope records to a Kubeflow profile namespace. */
export const MLMD_WORKSPACE_PROPERTY = '__kf_workspace__';

/**
 * gRPC methods that return tenant-scoped lineage data and must be constrained to
 * the caller's authorized namespace when multi-user auth is enabled.
 */
export const WORKSPACE_SCOPED_MLMD_METHODS = [
  'GetArtifacts',
  'GetExecutions',
  'GetContexts',
  'GetContextByTypeAndName',
  'GetArtifactsByContext',
  'GetExecutionsByContext',
  'GetChildrenContextsByContext',
  'GetArtifactsByType',
  'GetExecutionsByType',
] as const;

export type MlMetadataAuthorizedRequest = Request & {
  mlmdAuthorizedNamespace?: string;
};

export function buildWorkspaceFilterQuery(namespace: string): string {
  return `custom_properties.${MLMD_WORKSPACE_PROPERTY}.string_value = "${namespace}"`;
}

export function mlmdMethodRequiresWorkspaceScope(path: string): boolean {
  return WORKSPACE_SCOPED_MLMD_METHODS.some((method) => path.includes(method));
}

export function extractNamespaceFromRequest(
  request: Request,
  namespaceQueryKey = 'namespace',
): string | undefined {
  const rawNamespace = request.query[namespaceQueryKey];
  if (typeof rawNamespace === 'string' && rawNamespace) {
    return rawNamespace;
  }
  if (Array.isArray(rawNamespace) && rawNamespace[0]) {
    return String(rawNamespace[0]);
  }

  const headerNamespace = request.headers['kubeflow-namespace'];
  if (typeof headerNamespace === 'string' && headerNamespace) {
    return headerNamespace;
  }
  if (Array.isArray(headerNamespace) && headerNamespace[0]) {
    return String(headerNamespace[0]);
  }

  const referer = request.headers.referer;
  if (typeof referer === 'string' && referer) {
    try {
      const refererUrl = new URL(referer);
      const namespaceFromReferer = refererUrl.searchParams.get('ns');
      if (namespaceFromReferer) {
        return namespaceFromReferer;
      }
    } catch {
      // Ignore malformed referer values.
    }
  }

  return undefined;
}

/**
 * When multi-user auth is enabled, MLMD gRPC-web calls must identify the target
 * namespace so the shared metadata store cannot be enumerated across profiles.
 */
export function getMlMetadataAuthMiddleware(
  authorizeFn: AuthorizeFn,
  authEnabled: boolean,
  kubeflowUserIdHeader: string,
): Handler {
  return async (request: MlMetadataAuthorizedRequest, response: Response, next: NextFunction) => {
    if (!authEnabled) {
      return next();
    }

    const userIdHeader = kubeflowUserIdHeader.toLowerCase();
    const userId = request.headers[userIdHeader];
    if (!userId) {
      console.warn(
        `[SECURITY] Unauthenticated ML Metadata access attempt. Path: ${request.originalUrl}`,
      );
      response.status(401).send('Authentication required for ML Metadata access');
      return;
    }

    const namespace = extractNamespaceFromRequest(request);
    if (!namespace) {
      console.warn(
        `[SECURITY] Missing namespace for ML Metadata request. ` +
          `User: ${userId}, Path: ${request.originalUrl}`,
      );
      response
        .status(400)
        .send('Namespace parameter is required when authentication is enabled');
      return;
    }

    if (!isAllowedResourceName(namespace)) {
      console.warn(
        `[SECURITY] Invalid namespace for ML Metadata request. ` +
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

    request.mlmdAuthorizedNamespace = namespace;
    next();
  };
}
