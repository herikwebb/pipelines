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

const SAFE_METHODS = new Set(['GET', 'HEAD', 'OPTIONS']);

/**
 * Rejects state-changing requests that the browser marks as cross-site.
 *
 * Browsers set `Sec-Fetch-Site` on every request they make. `cross-site` means
 * the request was initiated by a page on another site, such as an
 * auto-submitted HTML form, while still carrying the user's session cookie and
 * therefore the identity the gateway derives from it. Requests from the UI
 * itself are `same-origin`, and non-browser clients (the kfp SDK, curl) do not
 * send the header at all, so neither is affected.
 */
export const rejectCrossSiteMutations: Handler = (req, res, next) => {
  if (SAFE_METHODS.has(req.method.toUpperCase())) {
    next();
    return;
  }
  const fetchSite = req.get('sec-fetch-site');
  if (fetchSite && fetchSite.trim().toLowerCase() === 'cross-site') {
    res
      .status(403)
      .type('text/plain')
      .send('Cross-site requests are not allowed for this endpoint.');
    return;
  }
  next();
};
