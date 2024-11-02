# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

### Added
- Added `README.md` and `CHANGELOG.md`
- Added pinging via CURL to `airflow\Airflow_DAGs_Provisioning\airflow_data_processing.py`

### Changed
- Minor fix in test configuration

## [0.0.5] - 2024-07-21

### Changed
- Fixed problems with the save paths
- naming convention in `airflow\Airflow_DAGs_Provisioning\airflow_data_processing.py`

## [0.0.4] - 2024-07-03

### Changed
- Changed the df download and upload functions to use (cloud-storage-wrapper)[https://github.com/ja-ba/Cloud-Storage-Wrapper] instead of using `utils.download_df()` and `utils.upload_df()`

### Removed
- The `utils.download_df()` and `utils.upload_df()` functions

## [0.0.3] - 2024-04-12

### Changed
- Avoided dropping index in `DailyLoader.create_wide_format()`

## [0.0.2] - 2024-03-30

### Changed
- Fix `DailyLoader` objects problems with empty datelists

## [0.0.1] - 2024-03-26

### Added
- Initial code base
