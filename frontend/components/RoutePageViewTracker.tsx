"use client";

import { useEffect, useRef } from "react";
import { usePathname, useSearchParams } from "next/navigation";
import { trackPageView } from "@/lib/analytics";

const SENSITIVE_QUERY_KEYS = new Set([
  "access_token",
  "auth",
  "code",
  "email",
  "key",
  "password",
  "secret",
  "session",
  "token",
]);

function buildAnalyticsPath(pathname: string, searchParams: URLSearchParams) {
  const safeSearchParams = new URLSearchParams();

  searchParams.forEach((value, key) => {
    if (!SENSITIVE_QUERY_KEYS.has(key.toLowerCase())) {
      safeSearchParams.append(key, value);
    }
  });

  const queryString = safeSearchParams.toString();

  return queryString ? `${pathname}?${queryString}` : pathname;
}

export function RoutePageViewTracker() {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const hasTrackedInitialPageView = useRef(false);

  useEffect(() => {
    if (!pathname) {
      return;
    }

    if (!hasTrackedInitialPageView.current) {
      hasTrackedInitialPageView.current = true;
      return;
    }

    trackPageView(buildAnalyticsPath(pathname, searchParams));
  }, [pathname, searchParams]);

  return null;
}
