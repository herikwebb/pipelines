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

import { describe, expect, it, vi } from 'vitest';
import express from 'express';
import requests from 'supertest';
import {
  getMlMetadataAuthMiddleware,
  isMlMetadataMutatingRpc,
  isMlMetadataUnscopedListRpc,
  ML_METADATA_NAMESPACE_HEADER,
} from './ml-metadata.js';
import {
  AuthorizeRequestResources,
  AuthorizeRequestVerb,
} from '../src/generated/apis/auth/index.js';

describe('ml-metadata auth helpers', () => {
  it('detects mutating ML Metadata RPC paths', () => {
    expect(
      isMlMetadataMutatingRpc('/ml_metadata.MetadataStoreService/PutArtifacts'),
    ).toBe(true);
    expect(
      isMlMetadataMutatingRpc('/ml_metadata.MetadataStoreService/DeleteExecutionType'),
    ).toBe(true);
    expect(
      isMlMetadataMutatingRpc('/ml_metadata.MetadataStoreService/GetArtifacts'),
    ).toBe(false);
  });

  it('detects unscoped list ML Metadata RPC paths', () => {
    expect(
      isMlMetadataUnscopedListRpc('/ml_metadata.MetadataStoreService/GetArtifacts'),
    ).toBe(true);
    expect(
      isMlMetadataUnscopedListRpc('/ml_metadata.MetadataStoreService/GetArtifactsByID'),
    ).toBe(false);
  });
});

describe('getMlMetadataAuthMiddleware', () => {
  function createApp(authEnabled: boolean) {
    const authorizeFn = vi.fn(async ({ namespace }) => {
      if (namespace === 'allowed-ns') {
        return undefined;
      }
      return { message: 'denied' };
    });
    const app = express();
    app.post(
      '/ml_metadata.MetadataStoreService/:method',
      getMlMetadataAuthMiddleware(authorizeFn, authEnabled, 'kubeflow-userid'),
      (_req, res) => {
        res.status(200).send('ok');
      },
    );
    return { app, authorizeFn };
  }

  it('blocks mutating RPCs even when auth is disabled', async () => {
    const { app } = createApp(false);
    await requests(app)
      .post('/ml_metadata.MetadataStoreService/PutArtifacts')
      .expect(403, 'ML Metadata write operations are not allowed through the UI');
  });

  it('allows scoped reads when auth is disabled', async () => {
    const { app } = createApp(false);
    await requests(app)
      .post('/ml_metadata.MetadataStoreService/GetArtifactsByID')
      .expect(200, 'ok');
  });

  it('requires authentication and namespace when auth is enabled', async () => {
    const { app } = createApp(true);
    await requests(app)
      .post('/ml_metadata.MetadataStoreService/GetArtifactsByID')
      .expect(401, 'Authentication required for ML Metadata access');
  });

  it('authorizes namespace access for scoped reads when auth is enabled', async () => {
    const { app, authorizeFn } = createApp(true);
    await requests(app)
      .post('/ml_metadata.MetadataStoreService/GetArtifactsByID')
      .set('kubeflow-userid', 'user@example.com')
      .set(ML_METADATA_NAMESPACE_HEADER, 'allowed-ns')
      .expect(200, 'ok');

    expect(authorizeFn).toHaveBeenCalledWith(
      {
        verb: AuthorizeRequestVerb.GET,
        resources: AuthorizeRequestResources.VIEWERS,
        namespace: 'allowed-ns',
      },
      expect.anything(),
    );
  });

  it('blocks unscoped list RPCs when auth is enabled', async () => {
    const { app } = createApp(true);
    await requests(app)
      .post('/ml_metadata.MetadataStoreService/GetArtifacts')
      .set('kubeflow-userid', 'user@example.com')
      .set(ML_METADATA_NAMESPACE_HEADER, 'allowed-ns')
      .expect(403, 'Unscoped ML Metadata list operations are not allowed through the UI');
  });
});
