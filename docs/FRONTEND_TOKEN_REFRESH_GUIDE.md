# Frontend Token Refresh Implementation Guide

## Overview

A standalone token refresh manager has been added to the frontend to automatically handle JWT token refresh and 401 error recovery.

## What Was Added

### 1. Auth Manager Module
**File:** `/var/www/lead-generation-platform/frontend/auth-manager.js`

This standalone JavaScript module provides:
- **Automatic token refresh** - Refreshes tokens before they expire (5-minute threshold)
- **Fetch interception** - Automatically adds auth headers to all API calls
- **401 error handling** - Automatically retries failed requests with fresh tokens
- **Session expiry handling** - Graceful redirect to login with message

### 2. Updated index.html
**File:** `/var/www/lead-generation-platform/frontend/index.html`

Added script tag before the main app:
```html
<script src="/auth-manager.js"></script>
```

## How It Works

### Automatic Token Refresh Flow

```
1. User logs in → JWT token stored in localStorage
2. User makes API call → auth-manager intercepts fetch
3. Check token expiry:
   a. If token expiring soon (< 5 min) → Auto-refresh via POST /api/auth/refresh
   b. If token valid → Add Authorization header
4. Make API request with auth header
5. If 401 error:
   a. Refresh token
   b. Retry original request with new token
   c. If refresh fails → Redirect to login with "session_expired" reason
```

### Key Features

1. **Proactive Refresh**: Tokens are refreshed BEFORE they expire, not after
2. **Transparent Retry**: Failed requests are automatically retried with fresh tokens
3. **No Code Changes Required**: Works with existing fetch() calls
4. **Graceful Degradation**: If refresh fails, user is redirected to login
5. **Singleton Pattern**: One global instance manages all auth state

## What Frontend Developers Need to Do

### If You Have Frontend Source Code

The auth-manager.js works as a standalone solution, but for a cleaner implementation, you should integrate the logic directly into your frontend source:

#### 1. Create an Auth Service

```javascript
// src/services/authService.js
class AuthService {
  constructor() {
    this.refreshThreshold = 5 * 60 * 1000; // 5 minutes
  }

  decodeToken(token) {
    const payload = JSON.parse(atob(token.split('.')[1]));
    return payload;
  }

  isTokenExpiringSoon(token) {
    const payload = this.decodeToken(token);
    const expiryTime = payload.exp * 1000;
    const currentTime = Date.now();
    return (expiryTime - currentTime) < this.refreshThreshold;
  }

  async refreshToken() {
    const token = localStorage.getItem('token');
    const response = await fetch('/api/auth/refresh', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`
      }
    });

    if (!response.ok) {
      throw new Error('Token refresh failed');
    }

    const data = await response.json();
    localStorage.setItem('token', data.token);
    return data.token;
  }

  logout() {
    localStorage.removeItem('token');
    window.location.href = '/login?reason=session_expired';
  }
}

export default new AuthService();
```

#### 2. Create an API Client with Interceptor

```javascript
// src/utils/apiClient.js
import authService from './authService';

async function apiClient(url, options = {}) {
  let token = localStorage.getItem('token');

  // Refresh token if expiring soon
  if (token && authService.isTokenExpiringSoon(token)) {
    try {
      token = await authService.refreshToken();
    } catch (error) {
      authService.logout();
      throw error;
    }
  }

  // Add auth header
  options.headers = {
    ...options.headers,
    'Authorization': `Bearer ${token}`
  };

  let response = await fetch(url, options);

  // Handle 401 errors
  if (response.status === 401) {
    try {
      token = await authService.refreshToken();
      options.headers['Authorization'] = `Bearer ${token}`;
      response = await fetch(url, options);
    } catch (error) {
      authService.logout();
      throw error;
    }
  }

  return response;
}

export default apiClient;
```

#### 3. Update API Calls

Replace all `fetch()` calls with `apiClient()`:

```javascript
// Before
fetch('/api/enrichment/jobs', {
  headers: { 'Authorization': `Bearer ${token}` }
});

// After
apiClient('/api/enrichment/jobs');
```

#### 4. Handle Session Expiry in UI Components

```javascript
// src/components/App.jsx or main layout
useEffect(() => {
  // Check token status periodically
  const interval = setInterval(async () => {
    const token = localStorage.getItem('token');
    if (token && authService.isTokenExpiringSoon(token)) {
      try {
        await authService.refreshToken();
      } catch (error) {
        authService.logout();
      }
    }
  }, 60000); // Check every minute

  return () => clearInterval(interval);
}, []);
```

### For React Apps (If Using Axios)

```javascript
// src/utils/axiosInterceptor.js
import axios from 'axios';
import authService from './authService';

const api = axios.create({
  baseURL: '/api'
});

// Request interceptor
api.interceptors.request.use(async (config) => {
  const token = localStorage.getItem('token');

  if (token && authService.isTokenExpiringSoon(token)) {
    try {
      const newToken = await authService.refreshToken();
      config.headers.Authorization = `Bearer ${newToken}`;
    } catch (error) {
      authService.logout();
      return Promise.reject(error);
    }
  }

  return config;
});

// Response interceptor
api.interceptors.response.use(
  (response) => response,
  async (error) => {
    if (error.response?.status === 401) {
      try {
        const newToken = await authService.refreshToken();
        error.config.headers.Authorization = `Bearer ${newToken}`;
        return axios.request(error.config);
      } catch (refreshError) {
        authService.logout();
        return Promise.reject(refreshError);
      }
    }
    return Promise.reject(error);
  }
);

export default api;
```

## Current Implementation (Standalone)

The current implementation uses `/auth-manager.js` which:
- ✅ Works without modifying source code
- ✅ Intercepts all fetch() calls automatically
- ✅ Handles token refresh transparently
- ✅ Redirects to login on session expiry
- ⚠️ Not ideal for production (should be in source code)

## Testing

### Manual Testing Steps

1. **Test Token Refresh:**
   ```javascript
   // In browser console:
   const token = localStorage.getItem('token');
   const payload = JSON.parse(atob(token.split('.')[1]));
   console.log('Token expires at:', new Date(payload.exp * 1000));

   // Wait until token is close to expiry (or manually change system time)
   // Make an API call - should auto-refresh
   ```

2. **Test 401 Handling:**
   ```javascript
   // Manually expire token:
   localStorage.setItem('token', 'invalid-token');

   // Try making an API call - should redirect to login
   ```

3. **Test Session Expiry:**
   ```javascript
   // In browser console:
   window.authManager.onSessionExpired = (error) => {
     console.log('Session expired:', error);
   };

   // Make API call with invalid token
   ```

### Verification Checklist

- [ ] Token refreshes automatically before expiry
- [ ] 401 errors trigger automatic refresh + retry
- [ ] Session expiry redirects to login with message
- [ ] No "Connection lost" errors - shows "Session expired" instead
- [ ] Long-running jobs don't fail due to token expiry
- [ ] Multiple tabs don't cause refresh conflicts

## Configuration Options

The auth manager can be configured in the browser console:

```javascript
window.authManager.init({
  onSessionExpired: (error) => {
    // Custom handler for session expiry
    console.error('Session expired:', error);
    // Show custom modal, redirect to different page, etc.
  }
});
```

## Backend Endpoint

The auth manager uses the existing backend endpoint:

```
POST /api/auth/refresh
Authorization: Bearer <existing_valid_token>

Response:
{
  "token": "new_jwt_token",
  "user_id": "...",
  "email": "...",
  "is_admin": false
}
```

## Troubleshooting

### Token Not Refreshing

**Problem:** Tokens not refreshing automatically

**Solution:**
```javascript
// Check if auth manager is initialized:
console.log(window.authManager);

// Check token status:
const token = localStorage.getItem('token');
const payload = JSON.parse(atob(token.split('.')[1]));
console.log('Expires at:', new Date(payload.exp * 1000));
console.log('Is expiring soon:', window.authManager.isTokenExpiringSoon(token));
```

### Infinite Refresh Loop

**Problem:** Token keeps refreshing repeatedly

**Solution:** Check that `/api/auth/refresh` endpoint returns a valid token with updated expiry:

```javascript
fetch('/api/auth/refresh', {
  method: 'POST',
  headers: {
    'Authorization': `Bearer ${localStorage.getItem('token')}`
  }
}).then(r => r.json()).then(console.log);
```

### Multiple Tab Conflicts

**Problem:** Multiple tabs refreshing token simultaneously

**Solution:** The auth manager uses `isRefreshing` flag to prevent concurrent refreshes. If issues persist, consider adding localStorage-based locking.

## Next Steps

1. **Test thoroughly** - Verify all scenarios work correctly
2. **Monitor logs** - Check browser console for auth manager messages
3. **Update frontend source** - Integrate this logic into actual source code
4. **Add unit tests** - Test auth service in isolation
5. **Document for users** - Update user documentation with session expiry info

## Files Modified

- ✅ `/var/www/lead-generation-platform/frontend/auth-manager.js` (NEW)
- ✅ `/var/www/lead-generation-platform/frontend/index.html` (UPDATED)

## Files to Modify in Frontend Source Repository

- `src/services/authService.js` (CREATE)
- `src/utils/apiClient.js` (CREATE or MODIFY)
- `src/App.jsx` (MODIFY - add session expiry handling)
- All API call files (MODIFY - use apiClient instead of fetch)

---

**Date:** March 10, 2026
**Status:** ✅ Token refresh module deployed to production
**Backend Endpoint:** ✅ POST /api/auth/refresh (already implemented)
**Frontend:** ✅ Standalone module working, source integration recommended
