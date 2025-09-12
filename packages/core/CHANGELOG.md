# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

# 0.1.0 (2025-09-12)

### Bug Fixes

* **core:** align get enqueue raw sql behavior with enqueue with empty array param ([7f1ede0](https://github.com/serenis-health/mysql-queue/commit/7f1ede095fa0908877f6a5217350d990614016c3))
* **core:** correct typos ([a921857](https://github.com/serenis-health/mysql-queue/commit/a921857980779701a302821067b0285abc926ee0))
* **core:** db use utc dates ([31170dd](https://github.com/serenis-health/mysql-queue/commit/31170dd693258f28a389b01b49ee99c4529496ff))
* **core:** enqueue throws if queue does not exists ([97eb1b0](https://github.com/serenis-health/mysql-queue/commit/97eb1b020c0238c9e3ab22be5afe57b497c40946))
* remove db query from job failed callback ([#41](https://github.com/serenis-health/mysql-queue/issues/41)) ([6349c6f](https://github.com/serenis-health/mysql-queue/commit/6349c6f168c9f51dbe45437c194a276262793cb3))

### Features

* add on job failed callback ([db15e04](https://github.com/serenis-health/mysql-queue/commit/db15e0450bbea879cd1bfedd1746df4187fe4410))
* **core:** add payload max size check ([5adce5c](https://github.com/serenis-health/mysql-queue/commit/5adce5c4e2b649436c4ef42ee6954cc14a4d702d))
* **core:** add tablesPrefix option ([0785962](https://github.com/serenis-health/mysql-queue/commit/0785962e746001c57e5a1567bdaf6f617faaa0d3))
* **core:** better indexes and mysql locks usage ([db78927](https://github.com/serenis-health/mysql-queue/commit/db78927a2685680a05fcc66b95f6ebdadd602741))
* **core:** get job execution promise with count ([51f3f31](https://github.com/serenis-health/mysql-queue/commit/51f3f31fa09734dcc86e410d625d25976ba57462))
* **core:** get promise for job execution ([8a93f6e](https://github.com/serenis-health/mysql-queue/commit/8a93f6ef1b05ef2c21f78e1030636aeca94d036a))
* **core:** replace connection with external db on enqueue method ([f768f1e](https://github.com/serenis-health/mysql-queue/commit/f768f1e04a0a04662463e94905b833faac99e1ca))
* **core:** truncate latestFailureReason ([f3a4e1c](https://github.com/serenis-health/mysql-queue/commit/f3a4e1c749f4fad6d0a75cac5fd67dbb6f11d8fd))
