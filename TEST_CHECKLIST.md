# Testing Checklist

Use this checklist to verify everything is working correctly.

## ✅ Pre-Testing Setup

- [ ] Python dependencies installed (`pip install -r config/requirements.txt`)
- [ ] `.env` file configured with Snowflake credentials
- [ ] `config/config.yaml` updated with your settings
- [ ] Snowflake account accessible

## ✅ Automated Tests

- [ ] Run automated test suite: `python scripts/test/test_pipeline.py`
- [ ] All critical tests pass (no failures)
- [ ] Review any warnings

## ✅ Setup Verification

- [ ] Run setup script: `python scripts/setup/run_setup.py`
- [ ] Warehouse created and configured
- [ ] Database and schemas created
- [ ] File formats created
- [ ] External stage configured
- [ ] Snowpipe created
- [ ] Validation tables created
- [ ] Stored procedures created
- [ ] Tasks created

## ✅ Connection Test

- [ ] Can connect to Snowflake
- [ ] Can query database
- [ ] Warehouse accessible

## ✅ File Ingestion Test

- [ ] Sample file uploaded to stage
- [ ] Snowpipe status checkable
- [ ] Data appears in staging table (if Snowpipe configured)
- [ ] File ingestion script runs without errors

## ✅ API Ingestion Test

- [ ] API endpoints configured in `config.yaml`
- [ ] API credentials set (if needed)
- [ ] API ingestion script runs
- [ ] Data appears in target tables
- [ ] Error handling works (test with invalid endpoint)

## ✅ Transformation Test

- [ ] `CLEAN_RAW_DATA` procedure executes
- [ ] Data appears in CLEANED schema
- [ ] `REMOVE_DUPLICATES` procedure executes
- [ ] `APPLY_BUSINESS_TRANSFORMS` procedure executes
- [ ] Data appears in ANALYTICS schema
- [ ] No errors in transformation logs

## ✅ Validation Test

- [ ] `VALIDATE_ROW_COUNT` executes
- [ ] `VALIDATE_NULL_PERCENTAGE` executes
- [ ] `VALIDATE_DATA_FRESHNESS` executes
- [ ] `RUN_ALL_VALIDATIONS` executes
- [ ] Validation results appear in VALIDATION.VALIDATION_RESULTS
- [ ] Validation logs appear in VALIDATION.PIPELINE_LOGS

## ✅ Orchestration Test

- [ ] Pipeline orchestrator script runs
- [ ] Individual steps can be run separately
- [ ] Full pipeline runs end-to-end
- [ ] Logs created for each step
- [ ] Errors handled gracefully

## ✅ Task Test

- [ ] Tasks visible in Snowflake
- [ ] Tasks can be resumed/suspended
- [ ] Task history accessible
- [ ] Tasks execute on schedule (or manually)

## ✅ Monitoring Test

- [ ] Pipeline logs queryable
- [ ] Validation results queryable
- [ ] Warehouse usage queryable
- [ ] Query history accessible
- [ ] Credit usage trackable

## ✅ Performance Test

- [ ] Warehouse auto-suspend works (wait 60+ seconds, verify suspended)
- [ ] Warehouse auto-resume works (submit query, verify resumes)
- [ ] Query performance acceptable
- [ ] No excessive credit usage

## ✅ End-to-End Test

- [ ] Complete pipeline run successful
- [ ] Data flows: RAW → CLEANED → ANALYTICS
- [ ] Validations pass
- [ ] Logs complete
- [ ] No errors in any step

## ✅ Error Handling Test

- [ ] Invalid file format handled gracefully
- [ ] API failures handled with retries
- [ ] Transformation errors logged
- [ ] Validation failures logged
- [ ] Pipeline stops on critical errors

## ✅ Documentation Review

- [ ] README.md reviewed
- [ ] DEPLOYMENT.md reviewed
- [ ] TESTING_GUIDE.md reviewed
- [ ] PERFORMANCE_GUIDE.md reviewed
- [ ] Configuration documented

## 📊 Test Results Summary

**Test Date**: _______________  
**Tested By**: _______________  

**Results**:
- Total Tests: _______
- Passed: _______
- Failed: _______
- Warnings: _______

**Notes**:
_________________________________________________
_________________________________________________
_________________________________________________

## 🎯 Sign-Off

- [ ] All critical tests passed
- [ ] Performance acceptable
- [ ] Documentation complete
- [ ] Ready for production use

**Approved By**: _______________  
**Date**: _______________

