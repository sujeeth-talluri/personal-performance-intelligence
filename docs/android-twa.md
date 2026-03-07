# Android Play Store Guide (Trusted Web Activity)

This guide converts hosted StrideIQ into an Android app package for Play Store.

## Prerequisites

- Hosted HTTPS app URL (example: `https://strideiq.yourdomain.com`)
- Android Studio installed
- Java 17+
- Node.js 20+
- Bubblewrap CLI

Install Bubblewrap:

```bash
npm i -g @bubblewrap/cli
```

## 1. Verify Web App Quality

- `manifest.json` is available at `/static/manifest.json`
- App works on mobile screen sizes
- HTTPS enabled
- OAuth redirect URI uses your production domain

## 2. Generate TWA Project

```bash
mkdir strideiq-android
cd strideiq-android
bubblewrap init --manifest https://strideiq.yourdomain.com/static/manifest.json
```

During prompts:

- Package id: `com.strideiq.app`
- App name: `StrideIQ`
- Start URL: `https://strideiq.yourdomain.com/`

## 3. Build APK / AAB

```bash
bubblewrap build
```

Output artifacts are generated in the project folder.

## 4. Verify on Android Device

```bash
bubblewrap install
```

Check:

- App opens full-screen as native shell
- Login and Strava OAuth callback works
- Dashboard loads for all users

## 5. Publish to Play Store

- Create app in Google Play Console
- Upload `.aab`
- Complete store listing, privacy policy, screenshots
- Roll out internal test first, then production

## OAuth Notes

For production OAuth callback:

- `STRAVA_REDIRECT_URI=https://strideiq.yourdomain.com/auth/strava/callback`
- Update same callback/domain in Strava developer app settings

## Recommendation

Ship PWA install first for fast testing with friends, then publish TWA when stable.
