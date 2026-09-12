"use client";

import { useEffect, useState } from "react";

const chevron = Array.from({ length: 9 }, (_, index) => {
  const row = Math.floor(index / 3);
  const column = index % 3;
  return (column + Math.abs(row - 1)) * 90;
});

const ORBIT_ORDER = [0, 1, 2, 5, 8, 7, 6, 3];
const orbit = Array.from({ length: 9 }, (_, index) => {
  const position = ORBIT_ORDER.indexOf(index);
  return position === -1 ? null : position * 110;
});

const PATTERNS: Record<string, { delays: (number | null)[]; duration: number; round: boolean }> = {
  Drive: { delays: chevron, duration: 650, round: false },
  Dots: { delays: chevron, duration: 650, round: true },
  Orbit: { delays: orbit, duration: 950, round: false },
};

function LoaderGrid({ delays, duration, round }: { delays: (number | null)[]; duration: number; round: boolean }) {
  return (
    <span aria-hidden="true" className="grid shrink-0 grid-cols-[repeat(3,4px)] gap-[1.5px]">
      {delays.map((delay, index) => (
        <span
          className={`size-[4px] bg-foreground ${round ? "rounded-full" : "rounded-[1px]"}`}
          key={index}
          style={{
            opacity: delay === null ? 0.07 : 0.15,
            animation: delay === null ? "none" : `pixel-on ${duration}ms ease-in-out ${delay}ms infinite`,
          }}
        />
      ))}
    </span>
  );
}

function useElapsed() {
  const [deciseconds, setDeciseconds] = useState(0);

  useEffect(() => {
    const timer = window.setInterval(() => setDeciseconds((current) => current + 1), 100);
    return () => window.clearInterval(timer);
  }, []);

  const total = deciseconds / 10;
  if (total < 60) return `${total.toFixed(1)}s`;
  return `${Math.floor(total / 60)}m ${(total % 60).toFixed(1)}s`;
}

export function LoadingState({
  label,
  variant = "Drive",
  videoSrc = "https://95dnc2a95qgwt9ff.public.blob.vercel-storage.com/subway-surfers.mp4",
}: {
  label?: string;
  variant?: string;
  videoSrc?: string;
}) {
  const elapsed = useElapsed();
  const surfer = variant === "Surfer";
  const resolvedLabel = label ?? (surfer ? "Subway surfing" : "Working");
  const [videoAvailable, setVideoAvailable] = useState(true);
  const { delays, duration, round } = PATTERNS[variant] ?? PATTERNS.Drive;

  const labelElement = <span className="loading-shimmer-label text-[13px] font-medium">{resolvedLabel}</span>;
  const elapsedElement = <span className="font-mono text-[12px] text-muted-foreground tabular-nums">{elapsed}</span>;

  if (surfer) {
    return (
      <div className="flex w-fit flex-col items-start" role="status">
        <div className="flex items-center gap-2.5">
          <LoaderGrid {...PATTERNS.Drive} />
          {labelElement}
          {elapsedElement}
        </div>
        <div className="mt-2 w-56 overflow-hidden rounded-[10px] shadow-xl [animation:pop-in_200ms_cubic-bezier(0.16,1,0.3,1)_both]">
          <div className="relative aspect-video w-full bg-zinc-950">
            {videoAvailable ? (
              <video autoPlay className="h-full w-full object-cover" loop muted onError={() => setVideoAvailable(false)} playsInline src={videoSrc} />
            ) : (
              <div className="flex h-full w-full flex-col items-center justify-center gap-1.5 text-zinc-400">
                <LoaderGrid {...PATTERNS.Drive} />
                <span className="px-3 text-center font-mono text-[10px]">Video unavailable</span>
              </div>
            )}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="flex w-fit items-center gap-2.5" role="status">
      <LoaderGrid delays={delays} duration={duration} round={round} />
      {labelElement}
      {elapsedElement}
    </div>
  );
}
