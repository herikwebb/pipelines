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

import express from 'express';
import request from 'supertest';
import { describe, expect, it, vi } from 'vitest';
import { getMlmdProxyAuthMiddleware } from './mlmd-proxy.js';

describe('getMlmdProxyAuthMiddleware', () => {
  const kubeflowUserIdHeader = 'x-goog-authenticated-user-email';

  function buildApp(authEnabled: boolean) {
    const app = express();
    const middleware = getMlmdProxyAuthMiddleware(authEnabled, kubeflowUserIdHeader);
    app.all('/ml_metadata.*', middleware, (_req, res) => {
      res.status(200).send('ok');
    });
    return app;
  }

  it('allows scoped reads when auth is disabled', async () => {
    const app = buildApp(false);
    const response = await request(app).post(
      '/ml_metadata.MetadataStoreService/GetExecutions',
    );
    expect(response.status).toBe(200);
  });

  it('rejects unauthenticated MLMD requests when auth is enabled', async () => {
    const app = buildApp(true);
    const response = await request(app).post(
      '/ml_metadata.MetadataStoreService/GetExecutionsByContext',
    );
    expect(response.status).toBe(401);
  });

  it('blocks global GetExecutions when auth is enabled', async () => {
    const app = buildApp(true);
    const response = await request(app)
      .post('/ml_metadata.MetadataStoreService/GetExecutions')
      .set(kubeflowUserIdHeader, 'user@example.com');
    expect(response.status).toBe(403);
    expect(response.text).toContain('GetExecutions');
  });

  it('blocks global GetArtifacts when auth is enabled', async () => {
    const app = buildApp(true);
    const response = await request(app)
      .post('/ml_metadata.MetadataStoreService/GetArtifacts')
      .set(kubeflowUserIdHeader, 'user@example.com');
    expect(response.status).toBe(403);
  });

  it('blocks PutExecutions when auth is enabled', async () => {
    const app = buildApp(true);
    const response = await request(app)
      .post('/ml_metadata.MetadataStoreService/PutExecutions')
      .set(kubeflowUserIdHeader, 'user@example.com');
    expect(response.status).toBe(403);
  });

  it('allows scoped GetExecutionsByContext when auth is enabled', async () => {
    const app = buildApp(true);
    const response = await request(app)
      .post('/ml_metadata.MetadataStoreService/GetExecutionsByContext')
      .set(kubeflowUserIdHeader, 'user@example.com');
    expect(response.status).toBe(200);
  });
});
