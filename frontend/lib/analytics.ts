type AnalyticsParamValue = string | number | boolean | null | undefined;

export type AnalyticsEventParams = Record<string, AnalyticsParamValue>;

type Gtag = (
  command: "config" | "event" | "js",
  eventName: string | Date,
  params?: Record<string, string | number | boolean | null>,
) => void;

declare global {
  interface Window {
    dataLayer?: unknown[];
    gtag?: Gtag;
  }
}

export const gaMeasurementId =
  process.env.NEXT_PUBLIC_GA_MEASUREMENT_ID?.trim();

function omitUndefinedParams(params?: AnalyticsEventParams) {
  if (!params) {
    return undefined;
  }

  return Object.fromEntries(
    Object.entries(params).filter(
      (entry): entry is [string, string | number | boolean | null] => {
        return entry[1] !== undefined;
      },
    ),
  );
}

export function trackEvent(name: string, params?: AnalyticsEventParams) {
  if (typeof window === "undefined" || !gaMeasurementId || !window.gtag) {
    return;
  }

  const eventParams = omitUndefinedParams(params);

  window.gtag("event", name, eventParams);

  if (process.env.NODE_ENV === "development") {
    console.debug("[analytics] event", name, eventParams ?? {});
  }
}

export function trackPageView(path: string) {
  if (typeof window === "undefined" || !gaMeasurementId || !window.gtag) {
    return;
  }

  window.gtag("config", gaMeasurementId, {
    page_path: path,
  });

  if (process.env.NODE_ENV === "development") {
    console.debug("[analytics] page_view", path);
  }
}
