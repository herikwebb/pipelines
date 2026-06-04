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

import { Handler, NextFunction, Response } from 'express';
import { createRequire } from 'module';
import {
  buildWorkspaceFilterQuery,
  MLMD_WORKSPACE_PROPERTY,
  mlmdMethodRequiresWorkspaceScope,
  MlMetadataAuthorizedRequest,
} from './ml-metadata-auth.js';

const require = createRequire(import.meta.url);
// eslint-disable-next-line @typescript-eslint/no-var-requires
const metadataStorePb = require('../../src/third_party/mlmd/generated/ml_metadata/proto/metadata_store_pb.js');
// eslint-disable-next-line @typescript-eslint/no-var-requires
const metadataStoreServicePb = require('../../src/third_party/mlmd/generated/ml_metadata/proto/metadata_store_service_pb.js');

type ListOptionsCarrier = {
  getOptions: () => { getFilterQuery: () => string; setFilterQuery: (value: string) => void } | undefined;
  setOptions: (value: unknown) => void;
  serializeBinary: () => Uint8Array;
};

type ListOptionsRequestType = {
  deserializeBinary: (bytes: Buffer) => ListOptionsCarrier;
};

const REQUEST_TYPES_BY_METHOD: Record<string, ListOptionsRequestType> = {
  GetArtifacts: metadataStoreServicePb.GetArtifactsRequest,
  GetExecutions: metadataStoreServicePb.GetExecutionsRequest,
  GetContexts: metadataStoreServicePb.GetContextsRequest,
  GetArtifactsByType: metadataStoreServicePb.GetArtifactsByTypeRequest,
  GetExecutionsByType: metadataStoreServicePb.GetExecutionsByTypeRequest,
};

export function extractGrpcWebMessage(grpcWebBody: Buffer): Buffer {
  if (grpcWebBody.length < 5) {
    return Buffer.alloc(0);
  }
  const messageLength = grpcWebBody.readUInt32BE(1);
  return grpcWebBody.subarray(5, 5 + messageLength);
}

export function wrapGrpcWebMessage(message: Buffer): Buffer {
  const frame = Buffer.alloc(5 + message.length);
  frame[0] = 0;
  frame.writeUInt32BE(message.length, 1);
  message.copy(frame, 5);
  return frame;
}

function mergeWorkspaceFilter(existingFilter: string | undefined, namespace: string): string {
  const workspaceFilter = buildWorkspaceFilterQuery(namespace);
  if (!existingFilter) {
    return workspaceFilter;
  }
  if (existingFilter.includes(MLMD_WORKSPACE_PROPERTY)) {
    return existingFilter;
  }
  return `(${existingFilter}) AND (${workspaceFilter})`;
}

function applyWorkspaceFilterToListRequest(
  requestMessage: ListOptionsCarrier,
  namespace: string,
): void {
  const workspaceFilter = buildWorkspaceFilterQuery(namespace);
  const listOptions =
    requestMessage.getOptions() ?? new metadataStorePb.ListOperationOptions();
  if (!requestMessage.getOptions()) {
    requestMessage.setOptions(listOptions);
  }
  const existingFilter = listOptions.getFilterQuery();
  listOptions.setFilterQuery(mergeWorkspaceFilter(existingFilter, namespace));
  if (!listOptions.getFilterQuery().includes(namespace)) {
    listOptions.setFilterQuery(workspaceFilter);
  }
}

export function injectWorkspaceFilterIntoGrpcWebBody(
  grpcWebBody: Buffer,
  namespace: string,
  methodPath: string,
): Buffer {
  if (!grpcWebBody.length || !mlmdMethodRequiresWorkspaceScope(methodPath)) {
    return grpcWebBody;
  }

  const methodName = Object.keys(REQUEST_TYPES_BY_METHOD).find((name) => methodPath.includes(name));
  if (!methodName) {
    return grpcWebBody;
  }

  const RequestType = REQUEST_TYPES_BY_METHOD[methodName];
  const messageBytes = extractGrpcWebMessage(grpcWebBody);

  const requestMessage = RequestType.deserializeBinary(messageBytes) as ListOptionsCarrier;
  applyWorkspaceFilterToListRequest(requestMessage, namespace);
  return wrapGrpcWebMessage(Buffer.from(requestMessage.serializeBinary()));
}

/**
 * Rewrites scoped MLMD gRPC-web request bodies to enforce the authorized workspace
 * filter before the request is proxied to metadata-envoy.
 */
export function getMlMetadataBodyMiddleware(authEnabled: boolean): Handler {
  return (request: MlMetadataAuthorizedRequest, response: Response, next: NextFunction) => {
    if (!authEnabled) {
      return next();
    }

    const namespace = request.mlmdAuthorizedNamespace;
    if (!namespace) {
      response.status(500).send('ML Metadata authorization context is missing');
      return;
    }

    if (request.method !== 'POST' || !mlmdMethodRequiresWorkspaceScope(request.path)) {
      return next();
    }

    const rawBody = request.body;
    if (!Buffer.isBuffer(rawBody)) {
      response.status(400).send('Invalid ML Metadata request body');
      return;
    }

    request.body = injectWorkspaceFilterIntoGrpcWebBody(rawBody, namespace, request.path);
    next();
  };
}
