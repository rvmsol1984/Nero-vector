import { useState } from "react";

// NERO brand logo. Loads /logo.png from the Vite public/ dir and falls
// back to a styled text wordmark if the file isn't present in the build
// (e.g. public/logo.png wasn't committed to the repo). The img tag keeps
// the brightness(0) invert(1) filter so a colour logo PNG still renders
// pure white on the dark UI surfaces.

export default function Logo({ size = 64 }) {
  const [failed, setFailed] = useState(false);

  if (failed) {
    return (
      <div
        className="font-bold tracking-[0.15em] text-white leading-none select-none"
        style={{ fontSize: Math.round(size * 0.55) }}
        aria-label="NERO"
      >
        NERO
      </div>
    );
  }

  return (
    <img
      src="/logo.png"
      alt="NERO"
      onError={() => setFailed(true)}
      style={{
        height: `${size}px`,
        width: "auto",
        filter: "brightness(0) invert(1)",
      }}
    />
  );
}
