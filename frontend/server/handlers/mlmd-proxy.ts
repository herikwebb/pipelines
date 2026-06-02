// Copyright 2025 The Kubeflow Authors
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

import { Handler, NextFunction, Request, Response } from 'express';

/**
 * MLMD gRPC-Web RPC names that enumerate or mutate the global metadata store.
 * These must not be proxied without namespace-scoped authorization because MLMD
 * is shared across all Kubeflow profiles in multi-user mode.
 */
const BLOCKED_MLMD_RPC_SUFFIXES_WHEN_AUTH_ENABLED = [
  'GetExecutions',
  'GetArtifacts',
  'GetContexts',
  'PutArtifactType',
  'PutExecutionType',
  'PutContextType',
  'PutTypes',
  'PutArtifacts',
  'PutExecutions',
  'PutContexts',
  'PutEvents',
  'PutAttributions',
  'PutParentContexts',
  'PutParentTypes',
  'DeleteArtifacts',
  'DeleteExecutions',
  'DeleteContexts',
  'DeleteEvents',
];

function getMlmdRpcName(path: string): string | undefined {
  const match = path.match(/\/ml_metadata\.MetadataStoreService\/([^/?]+)/);
  return match?.[1];
}

/**
 * Middleware for the ML Metadata gRPC-Web proxy.
 *
 * In multi-user mode the UI forwards /ml_metadata.* to a cluster-wide MLMD
 * instance. Without checks, any caller who can reach the UI can list every
 * execution and artifact or invoke write RPCs across all namespaces.
 */
export function getMlmdProxyAuthMiddleware(
  authEnabled: boolean,
  kubeflowUserIdHeader: string,
): Handler {
  return (request: Request, response: Response, next: NextFunction) => {
    if (!authEnabled) {
      return next();
    }

    const userId = request.headers[kubeflowUserIdHeader.toLowerCase()];
    if (!userId) {
      console.warn(
        `[SECURITY] Unauthenticated MLMD proxy access attempt. Path: ${request.originalUrl}`,
      );
      response.status(401).send('Authentication required for metadata access');
      return;
    }

    const rpcName = getMlmdRpcName(request.path);
    if (
      rpcName &&
      BLOCKED_MLMD_RPC_SUFFIXES_WHEN_AUTH_ENABLED.some(
        (blockedRpc) => blockedRpc === rpcName,
      )
    ) {
      console.warn(
        `[SECURITY] Blocked global MLMD RPC. User: ${userId}, RPC: ${rpcName}, Path: ${request.originalUrl}`,
      );
      response
        .status(403)
        .send(
          `Metadata RPC ${rpcName} is not allowed through the UI proxy in multi-user mode`,
        );
      return;
    }

    return next();
  };
}
