# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

# [0.8.0](https://github.com/serenis-health/mysql-queue/compare/0.7.0...0.8.0) (2025-11-02)

### Features

* **core:** add ability to include periodic schedule time in payload ([dfa39d8](https://github.com/serenis-health/mysql-queue/commit/dfa39d810ad28ec46344a6ea4cdfa009c0f8c389))
* **core:** add periodic jobs scheduler and simple leader election ([3cde71f](https://github.com/serenis-health/mysql-queue/commit/3cde71f78d876a2d96bf25726204ee68de35a881))
* **core:** introduce concurrent job processing ([7fcbf85](https://github.com/serenis-health/mysql-queue/commit/7fcbf85f591225e093123bebb4a26a888fa033b8))

# [0.7.0](https://github.com/serenis-health/mysql-queue/compare/0.6.1...0.7.0) (2025-10-16)

### Features

* **core:** change worker model from atomic completion to independent job processing ([46194a6](https://github.com/serenis-health/mysql-queue/commit/46194a6dbfc6b3fe7d241eb181b0dd6fcc4d59a0))

## [0.6.1](https://github.com/serenis-health/mysql-queue/compare/0.6.0...0.6.1) (2025-10-06)

### Bug Fixes

* **core:** upsert queue must not reset queue paused flag ([2f589b1](https://github.com/serenis-health/mysql-queue/commit/2f589b1fa56c1e02276495e88d851173096f32f2))
* **core:** wrong pending dedup key index ([a2578ba](https://github.com/serenis-health/mysql-queue/commit/a2578ba1e3e179fb9f6dfe2045c07383eec11c84))

# [0.6.0](https://github.com/serenis-health/mysql-queue/compare/0.5.1...0.6.0) (2025-10-02)

### Features

* **core:** pause queue ([6e5da97](https://github.com/serenis-health/mysql-queue/commit/6e5da9763567ed85c7b576ee995dd87700bc47b6))

## [0.5.1](https://github.com/serenis-health/mysql-queue/compare/0.5.0...0.5.1) (2025-10-01)

### Bug Fixes

* **core:** session execute use mysql connection query method ([b571cba](https://github.com/serenis-health/mysql-queue/commit/b571cba0877876955fb935fc0eb8d667eff66b5c))

# [0.5.0](https://github.com/serenis-health/mysql-queue/compare/0.3.0...0.5.0) (2025-10-01)

### Bug Fixes

* **core:** session methods returns ([7f7c903](https://github.com/serenis-health/mysql-queue/commit/7f7c90332d59b01d5c7f923b56df031b08a5eafd))

### Features

* **core:** add idempotence and pending deduplication ([d8635e8](https://github.com/serenis-health/mysql-queue/commit/d8635e8c5abcb2302acca41feffd4583fd4fa74e))

# [0.4.0](https://github.com/serenis-health/mysql-queue/compare/0.3.0...0.4.0) (2025-09-28)

### Features

* **core:** add idempotence and pending deduplication ([d8635e8](https://github.com/serenis-health/mysql-queue/commit/d8635e8c5abcb2302acca41feffd4583fd4fa74e))

# 0.3.0 (2025-09-28)

### Features

* **core:** improve session for insert or update queries ([6837ae6](https://github.com/serenis-health/mysql-queue/commit/6837ae65319dc4ee11415e9be07abe2ae8551cc0))

# 0.2.0 (2025-09-15)

### Bug Fixes

* **core:** align get enqueue raw sql behavior with enqueue with empty array param ([7f1ede0](https://github.com/serenis-health/mysql-queue/commit/7f1ede095fa0908877f6a5217350d990614016c3))
* **core:** correct typos ([a921857](https://github.com/serenis-health/mysql-queue/commit/a921857980779701a302821067b0285abc926ee0))
* **core:** db use utc dates ([31170dd](https://github.com/serenis-health/mysql-queue/commit/31170dd693258f28a389b01b49ee99c4529496ff))
* **core:** enqueue throws if queue does not exists ([97eb1b0](https://github.com/serenis-health/mysql-queue/commit/97eb1b020c0238c9e3ab22be5afe57b497c40946))
* **core:** non blocking callback abort due worker abort ([0f4e238](https://github.com/serenis-health/mysql-queue/commit/0f4e238babb7e87a45876e06e3500eac7b635395))
* remove db query from job failed callback ([#41](https://github.com/serenis-health/mysql-queue/issues/41)) ([6349c6f](https://github.com/serenis-health/mysql-queue/commit/6349c6f168c9f51dbe45437c194a276262793cb3))

### Features

* add on job failed callback ([db15e04](https://github.com/serenis-health/mysql-queue/commit/db15e0450bbea879cd1bfedd1746df4187fe4410))
* **core:** add partition concept ([738a67f](https://github.com/serenis-health/mysql-queue/commit/738a67fa0fcff99106b575db2bd06c3f12ada86b))
* **core:** add payload max size check ([5adce5c](https://github.com/serenis-health/mysql-queue/commit/5adce5c4e2b649436c4ef42ee6954cc14a4d702d))
* **core:** add tablesPrefix option ([0785962](https://github.com/serenis-health/mysql-queue/commit/0785962e746001c57e5a1567bdaf6f617faaa0d3))
* **core:** better indexes and mysql locks usage ([db78927](https://github.com/serenis-health/mysql-queue/commit/db78927a2685680a05fcc66b95f6ebdadd602741))
* **core:** get job execution promise with count ([51f3f31](https://github.com/serenis-health/mysql-queue/commit/51f3f31fa09734dcc86e410d625d25976ba57462))
* **core:** get promise for job execution ([8a93f6e](https://github.com/serenis-health/mysql-queue/commit/8a93f6ef1b05ef2c21f78e1030636aeca94d036a))
* **core:** handle lock during initial migrations ([f69fe7d](https://github.com/serenis-health/mysql-queue/commit/f69fe7dec90ccd30efab21adce29d4aa5d69cd40))
* **core:** replace connection with external db on enqueue method ([f768f1e](https://github.com/serenis-health/mysql-queue/commit/f768f1e04a0a04662463e94905b833faac99e1ca))
* **core:** truncate latestFailureReason ([f3a4e1c](https://github.com/serenis-health/mysql-queue/commit/f3a4e1c749f4fad6d0a75cac5fd67dbb6f11d8fd))
