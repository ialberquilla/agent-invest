import type { Metadata } from "next";
import { Inter, Manrope, Geist_Mono } from "next/font/google";
import Script from "next/script";
import { Suspense } from "react";
import "./globals.css";
import { RoutePageViewTracker } from "@/components/RoutePageViewTracker";
import { gaMeasurementId } from "@/lib/analytics";

const inter = Inter({
  variable: "--font-inter",
  subsets: ["latin"],
});

const manrope = Manrope({
  variable: "--font-manrope",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Pond3r | DeFi Allocation Copilot",
  description: "Agentic DeFi portfolio research and allocation workspace",
  icons: {
    icon: "/favicon.svg",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const shouldLoadGoogleAnalytics = Boolean(gaMeasurementId);

  return (
    <html
      lang="en"
      className={`${inter.variable} ${manrope.variable} ${geistMono.variable} h-full antialiased`}
      suppressHydrationWarning
    >
      <head>
        <script
          // Apply the saved theme before paint to avoid a flash. Defaults to
          // dark unless the user has explicitly chosen light.
          dangerouslySetInnerHTML={{
            __html: `try{var t=localStorage.getItem('agent-invest:theme');if(t!=='light')document.documentElement.classList.add('dark');}catch(e){document.documentElement.classList.add('dark');}`,
          }}
        />
      </head>
      <body className="min-h-full flex flex-col">
        {children}
        {shouldLoadGoogleAnalytics ? (
          <Suspense fallback={null}>
            <RoutePageViewTracker />
          </Suspense>
        ) : null}
      </body>
      {shouldLoadGoogleAnalytics ? (
        <>
          <Script
            src={`https://www.googletagmanager.com/gtag/js?id=${gaMeasurementId}`}
            strategy="afterInteractive"
          />
          <Script id="google-analytics" strategy="afterInteractive">
            {`
              window.dataLayer = window.dataLayer || [];
              function gtag(){window.dataLayer.push(arguments);}
              gtag('js', new Date());
              gtag('config', '${gaMeasurementId}');
            `}
          </Script>
        </>
      ) : null}
    </html>
  );
}
