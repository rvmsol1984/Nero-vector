/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,jsx}"],
  theme: {
    extend: {
      colors: {
        // page / surfaces
        bg:         "#0A0F1E",
        surface:    "#0D1428",
        card:       "#1a2235",
        elevated:   "#1e2d4a",

        // brand + status semantics (FieldDesk palette)
        primary:         "#2563EB",
        "primary-light": "#3B82F6",
        "primary-dark":  "#1D4ED8",

        critical: "#EF4444",
        high:     "#F97316",
        medium:   "#EAB308",
        low:      "#22C55E",

        "status-new":         "#3B82F6",
        "status-inprogress":  "#8B5CF6",
        "status-waiting":     "#F59E0B",
        "status-resolved":    "#10B981",
      },
      fontFamily: {
        sans: ["Inter", "ui-sans-serif", "system-ui", "sans-serif"],
      },
      borderRadius: {
        card:   "16px",
        button: "12px",
        input:  "12px",
      },
      keyframes: {
        "slide-up": {
          from: { transform: "translateY(8px)", opacity: "0" },
          to:   { transform: "translateY(0)",   opacity: "1" },
        },
        "fade-in": {
          from: { opacity: "0" },
          to:   { opacity: "1" },
        },
      },
      animation: {
        "slide-up": "slide-up 300ms ease-out",
        "fade-in":  "fade-in 200ms ease-out",
      },
    },
  },
  plugins: [],
};
