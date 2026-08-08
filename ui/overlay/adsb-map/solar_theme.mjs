const DEG_TO_RAD = Math.PI / 180;
const RAD_TO_DEG = 180 / Math.PI;
const DAY_MS = 86_400_000;

function clamp(value, minimum, maximum) {
  return Math.min(maximum, Math.max(minimum, value));
}

function normalizeDegrees(value) {
  return ((value % 360) + 360) % 360;
}

function normalizeSignedDegrees(value) {
  const normalized = normalizeDegrees(value);
  return normalized > 180 ? normalized - 360 : normalized;
}

function smoothstep(value) {
  const progress = clamp(value, 0, 1);
  return progress * progress * (3 - 2 * progress);
}

function interpolate(start, end, progress) {
  return start + (end - start) * smoothstep(progress);
}

export function solarPosition(date, latitude, longitude) {
  const epochMs = date instanceof Date ? date.getTime() : Number.NaN;
  if (!Number.isFinite(epochMs) || !Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    throw new TypeError("solarPosition requires a valid date, latitude, and longitude");
  }

  const julianDay = epochMs / DAY_MS + 2_440_587.5;
  const daysSinceJ2000 = julianDay - 2_451_545.0;
  const meanLongitude = normalizeDegrees(280.460 + 0.9856474 * daysSinceJ2000);
  const meanAnomaly = normalizeDegrees(357.528 + 0.9856003 * daysSinceJ2000) * DEG_TO_RAD;
  const eclipticLongitude = normalizeDegrees(
    meanLongitude + 1.915 * Math.sin(meanAnomaly) + 0.020 * Math.sin(2 * meanAnomaly),
  ) * DEG_TO_RAD;
  const obliquity = (23.439 - 0.0000004 * daysSinceJ2000) * DEG_TO_RAD;
  const rightAscension = normalizeDegrees(
    Math.atan2(Math.cos(obliquity) * Math.sin(eclipticLongitude), Math.cos(eclipticLongitude)) * RAD_TO_DEG,
  );
  const declination = Math.asin(Math.sin(obliquity) * Math.sin(eclipticLongitude));
  const siderealTime = normalizeDegrees(280.46061837 + 360.98564736629 * daysSinceJ2000);
  const hourAngleDegrees = normalizeSignedDegrees(siderealTime + longitude - rightAscension);
  const hourAngle = hourAngleDegrees * DEG_TO_RAD;
  const latitudeRadians = latitude * DEG_TO_RAD;
  const altitude = Math.asin(
    Math.sin(latitudeRadians) * Math.sin(declination)
      + Math.cos(latitudeRadians) * Math.cos(declination) * Math.cos(hourAngle),
  );
  const azimuth = Math.atan2(
    Math.sin(hourAngle),
    Math.cos(hourAngle) * Math.sin(latitudeRadians)
      - Math.tan(declination) * Math.cos(latitudeRadians),
  ) * RAD_TO_DEG + 180;

  return {
    altitudeDegrees: altitude * RAD_TO_DEG,
    azimuthDegrees: normalizeDegrees(azimuth),
  };
}

export function solarTheme(date, latitude, longitude) {
  const position = solarPosition(date, latitude, longitude);
  const laterPosition = solarPosition(new Date(date.getTime() + 5 * 60_000), latitude, longitude);
  const altitude = position.altitudeDegrees;
  const rising = laterPosition.altitudeDegrees > altitude;
  let phase;
  let brightness;

  if (altitude <= -6) {
    phase = "NIGHT";
    brightness = 0;
  } else if (altitude < 0) {
    phase = "TWILIGHT";
    brightness = interpolate(0, 0.055, (altitude + 6) / 6);
  } else if (altitude < 8) {
    phase = "GOLDEN_HOUR";
    brightness = altitude < 2.5
      ? interpolate(0.055, 0.08, altitude / 2.5)
      : interpolate(0.08, 0.10, (altitude - 2.5) / 5.5);
  } else {
    phase = "DAY";
    brightness = 0.10;
  }

  const peakWarmth = rising ? 0.060 : 0.085;
  let warmth = 0;
  if (altitude > -6 && altitude < 0) {
    warmth = interpolate(0, peakWarmth * 0.5, (altitude + 6) / 6);
  } else if (altitude >= 0 && altitude < 2.5) {
    warmth = interpolate(peakWarmth * 0.5, peakWarmth, altitude / 2.5);
  } else if (altitude >= 2.5 && altitude < 8) {
    warmth = interpolate(peakWarmth, 0, (altitude - 2.5) / 5.5);
  }

  return {
    phase,
    altitudeDegrees: altitude,
    azimuthDegrees: position.azimuthDegrees,
    rising,
    brightness,
    warmth,
    sampleTimeUtc: date.toISOString(),
  };
}
