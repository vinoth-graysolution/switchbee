# Changelog

All notable changes to the Switchbee backend and GVA Vendor Dashboard codebase will be documented in this file.

## [Unreleased] - 2026-06-09

### Added
- **New Candidate REST APIs (`switchbee/src/service.py`):**
  - Added `PUT /candidates/{phone}` (`update_candidate`) to update existing candidate details (name, role, phone, lang, tags, calls, lastContact, lastintent) on the server in `candidates.csv`.
  - Added `DELETE /candidates/{phone}` (`delete_candidate`) to remove candidate entries from `candidates.csv`.
- **New Campaign Management REST APIs (`switchbee/src/service.py`):**
  - Added `PUT /campaigns/{campaign_id}` (`update_campaign`) to update settings for an existing campaign (name, role, max retries, retry delay).
  - Added `DELETE /campaigns/{campaign_id}` (`delete_campaign`) to delete a campaign and recursively clean up its directory from disk.

### Changed
- **Campaign Candidates Upload API (`switchbee/src/service.py`):**
  - Enhanced `POST /campaigns/{campaign_id}/candidates` (`upload_campaign_candidates`) to support an optional `append: bool = False` query parameter. This allows appending new candidates to an existing campaign target list instead of overwriting.
  - Relaxed campaign status validation to allow uploading/modifying candidates in `scheduled` or `paused` campaigns in addition to `created` ones.
- **Campaign Creation API (`switchbee/src/service.py`):**
  - Updated `POST /campaigns` to capture the `scheduled_at` timestamp from campaign settings.
- **Uvicorn Server Startup (`switchbee/src/service.py`):**
  - Switched from a hardcoded app run command to dynamic import format `uvicorn.run("service:app", reload=True, app_dir=str(_SRC_DIR))` to support auto-reloading on file changes.

### Fixed
- **Campaign Statistics Bug (`12/4` Display Issue):** 
  - Modified the statistics computation in `switchbee/src/campaign_runner.py` to group results by candidate phone numbers. 
  - Instead of reporting raw call attempts (which counts every retry, e.g. 12 attempts for 4 candidates with Max Retries set to 2), it now calculates unique candidates attempted, completed, unanswered, and failed.
- **Candidate Edit API Crash (500 Internal Server Error):**
  - Updated the candidate update and delete endpoints in `switchbee/src/service.py` (`update_candidate` and `delete_candidate`) to sanitize parsed CSV row dictionaries.
  - Dict keys are now strictly filtered to match the expected CSV `fieldnames` schema, resolving the `ValueError: dict contains fields not in fieldnames: None` crash caused by extra blank columns or trailing commas in `candidates.csv`.
- **Candidate Segment/Checkbox Selection Bug:**
  - Fixed a bug in the frontend campaign creation flow (`Front End/src/vendor/OutboundPage.jsx`) where checked candidate selection boxes were ignored on launch.
  - The API call now respects `data.selectedPhones` to upload only the manually selected candidates for the campaign instead of falling back to all candidates.
- **Tunneling Configuration:**
  - Updated `.env` with a secure, authenticated ngrok tunnel URL configuration for stable WebSocket stream connections from Exotel.
