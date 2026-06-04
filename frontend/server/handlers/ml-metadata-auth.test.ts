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

import { describe, expect, it } from 'vitest';
import {
  buildWorkspaceFilterQuery,
  extractNamespaceFromRequest,
  mlmdMethodRequiresWorkspaceScope,
} from './ml-metadata-auth.js';
import { injectWorkspaceFilterIntoGrpcWebBody, wrapGrpcWebMessage } from './ml-metadata-body.js';
import { createRequire } from 'module';

const require = createRequire(import.meta.url);
// eslint-disable-next-line @typescript-eslint/no-var-requires
const metadataStoreServicePb = require('../../src/third_party/mlmd/generated/ml_metadata/proto/metadata_store_service_pb.js');
// eslint-disable-next-line @typescript-eslint/no-var-requires
const metadataStorePb = require('../../src/third_party/mlmd/generated/ml_metadata/proto/metadata_store_pb.js');

describe('ml-metadata auth helpers', () => {
  it('detects workspace scoped MLMD methods', () => {
    expect(mlmdMethodRequiresWorkspaceScope('/ml_metadata.MetadataStoreService/GetArtifacts')).toBe(
      true,
    );
    expect(
      mlmdMethodRequiresWorkspaceScope('/ml_metadata.MetadataStoreService/PutArtifacts'),
    ).toBe(false);
  });

  it('builds the workspace filter query', () => {
    expect(buildWorkspaceFilterQuery('user-a')).toBe(
      'custom_properties.__kf_workspace__.string_value = "user-a"',
    );
  });

  it('extracts namespace from query and referer', () => {
    expect(
      extractNamespaceFromRequest({
        query: { namespace: 'profile-a' },
        headers: {},
      } as any),
    ).toBe('profile-a');

    expect(
      extractNamespaceFromRequest({
        query: {},
        headers: { referer: 'https://kubeflow.example/_/pipeline/?ns=profile-b' },
      } as any),
    ).toBe('profile-b');
  });

  it('injects workspace filter into GetArtifacts gRPC-web bodies', () => {
    const request = new metadataStoreServicePb.GetArtifactsRequest();
    const grpcWebBody = wrapGrpcWebMessage(Buffer.from(request.serializeBinary()));
    const filteredBody = injectWorkspaceFilterIntoGrpcWebBody(
      grpcWebBody,
      'profile-a',
      '/ml_metadata.MetadataStoreService/GetArtifacts',
    );
    const message = metadataStoreServicePb.GetArtifactsRequest.deserializeBinary(
      filteredBody.subarray(5, 5 + filteredBody.readUInt32BE(1)),
    );
    expect(message.getOptions()?.getFilterQuery()).toContain('profile-a');
    expect(message.getOptions()?.getFilterQuery()).toContain('__kf_workspace__');
  });
});
