# Frontend Tier Selector Implementation

## Overview
This document summarizes the frontend implementation for the tier selector that allows users to control coverage when selecting "All United States" in the scraper.

## What Was Implemented

### Frontend Changes (`frontend/index.html`)

#### 1. Coverage Level Selector (Lines 334-352)
Added a dropdown selector that only appears for US "all" mode with three options:
- **Standard: Significant Cities (Recommended)** - Tier 2 (241 cities, ~723 API calls)
- **Quick: Major Cities Only** - Tier 1 (43 cities, ~129 API calls)  
- **Comprehensive: All Cities** - Tier 3 (29,540 cities, ~88,620 API calls)

#### 2. Info Box Display
Shows real-time information about the selected tier:
- Number of cities that will be searched
- Estimated number of API calls
- Description of what the tier covers
- Color-coded border (green for Tier 1, blue for Tier 2, purple for Tier 3)

#### 3. JavaScript Logic (Lines 1385-1462)
- **Event Handler for Mode Change**: Shows/hides coverage selector when switching between "all" and other modes
- **Event Handler for Country Change**: Shows/hides coverage selector when switching between US and other countries
- **Event Handler for Coverage Level Change**: Updates info box when user changes tier
- **updateCoverageInfo() Function**: Updates the display with current tier information

#### 4. API Integration (Lines 1729-1736)
When creating a scraper job, if mode is "all" and country is "us", the tier is extracted and sent to the API:
```javascript
if (mode === 'all' && country === 'us') {
    const coverageLevel = document.getElementById('scraperCoverageLevel');
    if (coverageLevel && coverageLevel.value) {
        payload.tier = parseInt(coverageLevel.value.split('_')[1]);
    }
}
```

#### 5. CSS Styling (Lines 102-108)
Added styling for the info-icon (ℹ️) that provides hover tooltips.

## User Experience

### When to Use Each Tier

| Tier | Cities | API Calls | Best For |
|------|--------|-----------|----------|
| Tier 1 | 43 | ~129 | Quick scans when you need results fast |
| Tier 2 | 241 | ~723 | Standard business searches (recommended) |
| Tier 3 | 29,540 | ~88,620 | Research projects when you need everything |

### How It Works

1. User selects **United States** as country
2. User selects **All Regions** as search mode
3. **Coverage Level** dropdown appears below the mode selector
4. User sees real-time updates of cities and API calls
5. User can change tier to see different estimates
6. When job is started, selected tier is sent to backend

### For Other Countries

The coverage selector only appears for:
- Country: United States (us)
- Mode: All Regions

For all other countries and modes, the selector is hidden and doesn't affect the workflow.

## Testing

To test the implementation:

1. Navigate to https://listbuilding.eagleinfoservice.com/
2. Log in with your credentials
3. Select "United States" as country
4. Select "All Regions" as search mode
5. The "Coverage Level" selector should appear
6. Try changing the tier to see the info box update
7. The selected tier will be used when you start the job

## Bug Fixes

### Fixed JavaScript Error (Line 1442)
Changed `document('coverageTasks')` to `document.getElementById('coverageTasks')` to prevent runtime error.

## Backend Compatibility

The frontend is fully compatible with the backend implementation in `backend/scraper/centers.py`:
- `get_all_mode_tiers()` - Returns tier information
- `get_default_tier()` - Returns default tier (2)
- `get_centers_for_job()` - Accepts tier parameter
- `/regions/estimate` endpoint - Returns tier configuration

## Files Modified

1. `/var/www/lead-generation-platform/frontend/index.html`
   - Added coverage selector HTML
   - Added JavaScript event handlers
   - Added CSS for info icon
   - Fixed JavaScript bug

## Deployment

The service was restarted on 2026-06-06 at 06:46 UTC and is running successfully:
```
● lead-generation-platform.service - active (running)
```

## Status: ✅ COMPLETE

The tier selector is now fully functional in the frontend. Users can:
1. See the coverage selector when appropriate
2. Select their desired coverage level
3. See real-time estimates of cities and API calls
4. Have their selection properly sent to the backend
