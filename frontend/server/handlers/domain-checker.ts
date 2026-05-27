// Copyright 2023 The Kubeflow Authors
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

import net from 'net';

export function isAllowedDomain(urlStr: string, allowedDomain: string): boolean {
  let url: URL;
  try {
    url = new URL(urlStr);
  } catch (_) {
    console.log(`Domain not allowed: ${urlStr}`);
    return false;
  }

  const domain = url.hostname.toLowerCase();
  if (isBlockedHost(domain)) {
    console.log(`Domain not allowed: ${urlStr}`);
    return false;
  }

  let allowedRegExp: RegExp;
  try {
    allowedRegExp = new RegExp(allowedDomain);
  } catch (_) {
    console.log(`Invalid allowed domain regex: ${allowedDomain}`);
    return false;
  }

  const allowed = allowedRegExp.test(domain);
  if (!allowed) {
    console.log(`Domain not allowed: ${urlStr}`);
  }
  return allowed;
}

function isBlockedHost(host: string): boolean {
  if (host === 'localhost' || host.endsWith('.localhost')) {
    return true;
  }

  const ipVersion = net.isIP(host);
  if (!ipVersion) {
    return false;
  }

  return ipVersion === 4 ? isBlockedIPv4(host) : isBlockedIPv6(host);
}

function isBlockedIPv4(host: string): boolean {
  const octets = host.split('.').map((octet) => Number(octet));
  const [first, second] = octets;
  return (
    first === 0 ||
    first === 10 ||
    first === 127 ||
    (first === 169 && second === 254) ||
    (first === 172 && second >= 16 && second <= 31) ||
    (first === 192 && second === 168)
  );
}

function isBlockedIPv6(host: string): boolean {
  const normalizedHost = host.toLowerCase();
  return (
    normalizedHost === '::' ||
    normalizedHost === '::1' ||
    normalizedHost.startsWith('fc') ||
    normalizedHost.startsWith('fd') ||
    normalizedHost.startsWith('fe8') ||
    normalizedHost.startsWith('fe9') ||
    normalizedHost.startsWith('fea') ||
    normalizedHost.startsWith('feb')
  );
}
