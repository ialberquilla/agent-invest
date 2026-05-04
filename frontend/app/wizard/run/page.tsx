import { Suspense } from "react";

import { WizardRunView } from "@/components/WizardRunView";

export default function WizardRunPage() {
  return (
    <Suspense fallback={null}>
      <WizardRunView />
    </Suspense>
  );
}
