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

import express from 'express';
import * as http from 'http';
import { AddressInfo } from 'net';
import requests from 'supertest';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { getArtifactsProxyHandler, stripForwardedCredentialHeaders } from './artifacts.js';

describe('artifacts proxy credential handling', () => {
  let upstream: http.Server | undefined;

  afterEach(async () => {
    await new Promise<void>((resolve) => (upstream ? upstream.close(() => resolve()) : resolve()));
    upstream = undefined;
  });

  it('strips session, authorization and identity headers', () => {
    const removeHeader = vi.fn();

    stripForwardedCredentialHeaders({ removeHeader }, ['kubeflow-userid', '']);

    const removed = removeHeader.mock.calls.map(([name]) => name);
    expect(removed).toEqual(
      expect.arrayContaining([
        'cookie',
        'authorization',
        'proxy-authorization',
        'x-forwarded-access-token',
        'x-auth-request-access-token',
        'kubeflow-userid',
      ]),
    );
    expect(removed).not.toContain('');
  });

  it('does not forward caller credentials to the namespaced artifact service', async () => {
    let receivedHeaders: http.IncomingHttpHeaders | undefined;
    upstream = http.createServer((req, res) => {
      receivedHeaders = req.headers;
      res.writeHead(200, { 'content-type': 'text/plain' });
      res.end('artifact');
    });
    await new Promise<void>((resolve) => upstream!.listen(0, '127.0.0.1', resolve));
    const { port } = upstream.address() as AddressInfo;

    const app = express();
    app.get(
      '/artifacts/*',
      getArtifactsProxyHandler({
        enabled: true,
        allowedDomain: '^127\\.0\\.0\\.1(:\\d+)?$',
        namespacedServiceGetter: () => `http://127.0.0.1:${port}`,
        identityHeaders: ['kubeflow-userid'],
      }),
    );

    await requests(app)
      .get('/artifacts/get?source=volume&bucket=data&key=output.txt&namespace=tenant-ns')
      .set('Cookie', 'oauth2_proxy_kubeflow=session-secret')
      .set('Authorization', 'Bearer user-token')
      .set('kubeflow-userid', 'victim@example.com')
      .set('x-forwarded-access-token', 'access-token')
      .expect(200);

    expect(receivedHeaders).toBeDefined();
    expect(receivedHeaders!.cookie).toBeUndefined();
    expect(receivedHeaders!.authorization).toBeUndefined();
    expect(receivedHeaders!['kubeflow-userid']).toBeUndefined();
    expect(receivedHeaders!['x-forwarded-access-token']).toBeUndefined();
  });
});
