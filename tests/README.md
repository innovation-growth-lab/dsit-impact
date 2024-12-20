# Testing Documentation

This directory contains the test suite for the DSIT Impact project. The tests are organised to mirror the project's pipeline structure and use pytest as the testing framework.

## Test Structure

```
tests/
├── conftest.py              # Shared pytest fixtures and configuration
├── pipelines/              # Tests for individual pipelines
│   ├── data_collection_gtr/    # GTR data collection tests
│   │   ├── test_unit.py           # Unit tests
│   │   └── test_integration.py    # Integration tests
│   ├── data_collection_oa/     # OpenAlex data collection tests
│   ├── data_collection_s2/     # Semantic Scholar data collection tests
│   ├── data_processing_pdfs/   # PDF processing tests
│   ├── data_processing_authors/ # Author processing tests
│   └── data_results_team_metrics/ # Team metrics tests
```

## Test Categories

### Unit Tests (`test_unit.py`)
- Test individual functions and nodes in isolation
- Mock external dependencies (APIs, file systems)
- Focus on input/output validation and edge cases
- Quick to run and diagnose

### Integration Tests (`test_integration.py`)
- Test complete pipeline flows
- Use real data samples
- Verify data transformations across multiple nodes
- Test interactions between components

## Running Tests

1. **Run all tests**:
   ```bash
   pytest tests/
   ```

2. **Run specific test file**:
   ```bash
   pytest tests/pipelines/data_collection_oa/test_unit.py
   ```

3. **Run tests with coverage**:
   ```bash
   pytest --cov=src tests/
   ```

## Test Data

- Test fixtures use small, representative data samples
- Sample data is generated programmatically where possible
- Larger test datasets are stored in `conf/test/`

## Troubleshooting

Common issues and solutions:
1. **Slow Tests**
   - Check fixture scopes
   - Mock expensive operations
   - Use parallel test execution

2. **Flaky Tests**
   - Add proper wait conditions
   - Handle asynchronous operations
   - Add retry mechanisms for external services

3. **Memory Issues**
   - Use smaller data samples
   - Clean up test data
   - Monitor memory usage in CI/CD

For more information, refer to:
- [pytest documentation](https://docs.pytest.org/)
- [Kedro testing guide](https://docs.kedro.org/en/stable/development/test_development.html)
