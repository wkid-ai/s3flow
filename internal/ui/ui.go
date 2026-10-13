// Package ui serves the embedded management web UI. The UI is a static
// single-page application that talks to the authenticated management API; it
// holds no secrets and the page itself is served without authentication.
package ui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static
var assets embed.FS

// csp forbids inline script/style and any network access other than this origin.
const csp = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; " +
	"connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

// Handler serves the UI below /ui/.
func Handler() http.Handler {
	sub, err := fs.Sub(assets, "static")
	if err != nil {
		panic(err)
	}
	files := http.StripPrefix("/ui/", http.FileServer(http.FS(sub)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", csp)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cache-Control", "no-store")
		files.ServeHTTP(w, r)
	})
}
