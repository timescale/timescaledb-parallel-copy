# Changelog

## [0.13.1](https://github.com/timescale/timescaledb-parallel-copy/compare/v0.13.0...v0.13.1) (2026-10-15)


### Bug Fixes

* **deps:** update module github.com/docker/docker to v28 [security] ([#164](https://github.com/timescale/timescaledb-parallel-copy/issues/164)) ([f7022ea](https://github.com/timescale/timescaledb-parallel-copy/commit/f7022ead1886d22e3f0a06450e13927a15352eb4))
* **deps:** update module github.com/jackc/pgx/v5 to v5.9.2 [security] ([#163](https://github.com/timescale/timescaledb-parallel-copy/issues/163)) ([983f618](https://github.com/timescale/timescaledb-parallel-copy/commit/983f618dd6357962f31e5928f96b03df0367fa84))

## [0.13.0](https://github.com/timescale/timescaledb-parallel-copy/compare/v0.12.0...v0.13.0) (2026-05-15)


### Features

* extra info in duplicate database columns in mapping ([c33a859](https://github.com/timescale/timescaledb-parallel-copy/commit/c33a859b9d3fe883f3146bb4baeca619992ed189))
* extra info in duplicate database columns in mapping ([8ab274a](https://github.com/timescale/timescaledb-parallel-copy/commit/8ab274ad27a863d2243b9f6f200013c2f817553d))
* handle Windows-1252 encoded CSV data for UTF-8 targets ([#136](https://github.com/timescale/timescaledb-parallel-copy/issues/136)) ([78a24cd](https://github.com/timescale/timescaledb-parallel-copy/commit/78a24cd1ad8be8b43f22f17e6b9027896bf6c978))
* retry when encountering recoverable errors ([#135](https://github.com/timescale/timescaledb-parallel-copy/issues/135)) ([e0ff4d8](https://github.com/timescale/timescaledb-parallel-copy/commit/e0ff4d8445e00f6bc0486d9d901b4d26e923a2e9))


### Bug Fixes

* correct disable-direct-compress logic ([#142](https://github.com/timescale/timescaledb-parallel-copy/issues/142)) ([387bfc2](https://github.com/timescale/timescaledb-parallel-copy/commit/387bfc26f74895890f6c431b594d7efb7c8b37dc))
* treat query cancelled as recoverable error ([#148](https://github.com/timescale/timescaledb-parallel-copy/issues/148)) ([4c6e069](https://github.com/timescale/timescaledb-parallel-copy/commit/4c6e069d5e33e9b9039b0e91232c84f110bc7515))
