# Vendored browser libraries

Pinned, pre-built files served from `/app/vendor/` by `include_str!`.
No Node/bundler is used at build or runtime; CSP stays `script-src 'self'`
and no external resource is loaded.

| File | Library | Version | License | Source |
|---|---|---|---|---|
| `marked.min.js` | marked | 18.3.0 | MIT (`LICENSE-marked`) | https://cdn.jsdelivr.net/npm/marked@18.3.0/lib/marked.umd.min.js |
| `purify.min.js` | DOMPurify | 3.5.0 | Apache-2.0 / MPL-2.0 (`LICENSE-dompurify`) | https://cdn.jsdelivr.net/npm/dompurify@3.5.0/dist/purify.min.js |

Notes:

- `marked.min.js` is the UMD build (`lib/marked.umd.js` in the npm package),
  minified by jsDelivr with Terser as served at the source URL above; it
  exposes the global `marked` with `marked.parse(...)`.
- `purify.min.js` is the official minified dist file; it exposes the global
  `DOMPurify` with `DOMPurify.sanitize(...)` and `DOMPurify.addHook(...)`.

## SHA-256

```
4d31a4e7ba7f4e169860f1e85b991639cb04d457b0b0204dbb0723b3871e0c90  marked.min.js
f69a46022ab1f53e969c8ebbca98172c8995785a9b3be72ea4a12d25b1a1bf67  purify.min.js
8e3a3f82f59a60958f56ca08f445647c32a4733dc7ca6c2c46f6eb898471ab9c  LICENSE-marked
cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30  LICENSE-dompurify
```

Verify with: `sha256sum marked.min.js purify.min.js LICENSE-marked LICENSE-dompurify`
(run inside this directory; `README.md` itself is not listed above).
