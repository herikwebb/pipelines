// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
import { afterEach, describe, expect, it, vi } from 'vitest';
import { fetchHttpArtifact } from './artifacts.js';

describe('fetchHttpArtifact', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('rejects HTTP redirect responses instead of following them', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => new Response(null, { status: 302, headers: { Location: 'http://169.254.169.254/' } })),
    );

    await expect(fetchHttpArtifact('https://allowed.example/artifact', {})).rejects.toThrow(
      'Redirects are not allowed',
    );
    expect(fetch).toHaveBeenCalledWith('https://allowed.example/artifact', {
      headers: {},
      redirect: 'manual',
    });
  });

  it('returns successful responses when no redirect occurs', async () => {
    const body = new ReadableStream({
      start(controller) {
        controller.enqueue(new TextEncoder().encode('ok'));
        controller.close();
      },
    });
    vi.stubGlobal('fetch', vi.fn(async () => new Response(body, { status: 200 })));

    const response = await fetchHttpArtifact('https://allowed.example/artifact', {});
    expect(response.status).toBe(200);
  });
});
