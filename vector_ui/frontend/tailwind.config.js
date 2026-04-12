/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,jsx}"],
  theme: {
    extend: {
      colors: {
        bg:       "#07090d",
        surface:  "#0d1117",
        border:   "#161b22",
        accent:   "#58a6ff",
        critical: "#f85149",
        success:  "#3fb950",
        warning:  "#d29922",
        muted:    "#8b949e",
      },
      fontFamily: {
        mono:    ['"JetBrains Mono"', "ui-monospace", "monospace"],
        display: ['"Syne"', "ui-sans-serif", "sans-serif"],
      },
    },
  },
  plugins: [],
};
