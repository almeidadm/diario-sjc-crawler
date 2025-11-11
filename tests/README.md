# Tests

This directory contains pytest tests for the crawler using [vcrpy](https://github.com/kevin1024/vcrpy) to record and replay HTTP requests.

## Setup

Install test dependencies:

```bash
poetry install --with dev
```

## Running Tests

Run all tests:

```bash
pytest tests/
```

Run specific test file:

```bash
pytest tests/test_client.py
```

Run with verbose output:

```bash
pytest tests/ -v
```

## VCRpy Usage

The tests use `vcrpy` to record HTTP interactions and replay them during test runs. This provides several benefits:

1. **Speed**: Tests run faster by using recorded responses instead of making real HTTP requests
2. **Reliability**: Tests don't depend on network connectivity or API availability
3. **Determinism**: Tests produce consistent results regardless of API state
4. **Offline Testing**: Tests can run without internet connection after initial recording

### Recording Mode

VCRpy is configured with `record_mode="once"`, which means:

- **First run**: Makes real HTTP requests and records them to YAML files in `tests/fixtures/vcr_cassettes/`
- **Subsequent runs**: Uses the recorded responses from the YAML files

### Cassette Files

Recorded HTTP interactions are stored in `tests/fixtures/vcr_cassettes/` as YAML files. These files contain:

- Request details (method, URL, headers, body)
- Response details (status, headers, body)
- Timing information

### Updating Recordings

If you need to update the recorded responses (e.g., API response format changed):

1. Delete the relevant cassette file from `tests/fixtures/vcr_cassettes/`
2. Run the test again - it will make a new HTTP request and record it

Or temporarily change `record_mode` to `"new_episodes"` or `"all"` in the test file.

## Test Structure

- `test_client.py`: Tests for `HttpClient` class
- `test_concurrent.py`: Tests for `ConcurrentHttpClient` class
- `test_crawler.py`: Tests for `GazetteCrawler` class (main crawler)

## Mock Storage

The crawler tests use a `MockStorage` class that stores editions in memory instead of writing to disk. This makes tests faster and avoids file system dependencies.

## Notes

- Tests that interact with the real API (in `test_crawler.py`) will record actual API responses
- Some tests may be skipped if the API doesn't return data for the test date range
- The test dates are chosen to minimize API calls while still testing the full workflow

