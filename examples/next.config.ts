import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Bake the build's id in (CI sets NEXT_DEPLOYMENT_ID to the commit SHA). Next then tags static
  // assets with `?dpl=<id>` and sends `x-deployment-id` on client navigations and prefetches;
  // skew-router uses that to keep a tab on the version that served it. Unset in dev = off.
  deploymentId: process.env.NEXT_DEPLOYMENT_ID || undefined,
};

export default nextConfig;
