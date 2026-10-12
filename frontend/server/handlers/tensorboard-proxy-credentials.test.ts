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
import requests from 'supertest';
import { describe, expect, it, vi } from 'vitest';

const proxyOptions: any[] = [];

// Capture the proxy configuration instead of dialing an in-cluster viewer service.
vi.mock('http-proxy-middleware', () => ({
  createProxyMiddleware: (options: any) => {
    proxyOptions.push(options);
    return (_req: express.Request, res: express.Response) => {
      res.status(200).send('proxied');
    };
  },
}));

const { default: registerTensorboardProxy, createTensorboardProxyPath } =
  await import('./tensorboard-proxy.js');

const TENSORBOARD_PROXY_SIGNING_SECRET = 'tensorboard-proxy-test-secret';

describe('tensorboard-proxy credential handling', () => {
  it('registers hooks that keep caller credentials and viewer cookies out of the proxy', async () => {
    const app = express();
    registerTensorboardProxy(
      app,
      '/pipeline',
      {
        clusterDomain: '.svc.cluster.local',
        proxySigningSecret: TENSORBOARD_PROXY_SIGNING_SECRET,
        tfImageName: 'tensorflow/tensorflow',
      },
      vi.fn(async () => undefined),
      ['kubeflow-userid'],
    );
    const proxyPath = createTensorboardProxyPath(
      'test-ns',
      'viewer-abcdefg',
      TENSORBOARD_PROXY_SIGNING_SECRET,
    );

    await requests(app)
      .get(`/${proxyPath}`)
      .set('Cookie', 'oauth2_proxy=secret')
      .set('kubeflow-userid', 'victim@example.com')
      .expect(200, 'proxied');

    expect(proxyOptions).toHaveLength(1);
    const { on } = proxyOptions[0];

    const removeHeader = vi.fn();
    on.proxyReq({ removeHeader });
    const removed = removeHeader.mock.calls.map(([name]) => name);
    expect(removed).toEqual(expect.arrayContaining(['cookie', 'authorization', 'kubeflow-userid']));

    const proxyRes = { headers: { 'set-cookie': ['planted=1'], 'content-type': 'text/html' } };
    on.proxyRes(proxyRes);
    expect(proxyRes.headers).toEqual({ 'content-type': 'text/html' });
  });
});
