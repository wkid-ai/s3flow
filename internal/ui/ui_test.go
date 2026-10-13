package ui_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"s3qos/internal/ui"
)

func get(t *testing.T, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	ui.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestAssetsServedWithStrictHeaders(t *testing.T) {
	for path, ctype := range map[string]string{
		"/ui/":          "text/html",
		"/ui/app.js":    "javascript",
		"/ui/lib.js":    "javascript",
		"/ui/style.css": "text/css",
	} {
		rec := get(t, path)
		if rec.Code != 200 {
			t.Fatalf("%s: status %d", path, rec.Code)
		}
		if !strings.Contains(rec.Header().Get("Content-Type"), ctype) {
			t.Errorf("%s: content-type %q", path, rec.Header().Get("Content-Type"))
		}
		csp := rec.Header().Get("Content-Security-Policy")
		for _, want := range []string{"default-src 'none'", "script-src 'self'", "style-src 'self'", "connect-src 'self'", "frame-ancestors 'none'"} {
			if !strings.Contains(csp, want) {
				t.Errorf("%s: CSP missing %q (%s)", path, want, csp)
			}
		}
		if strings.Contains(csp, "unsafe-inline") || strings.Contains(csp, "unsafe-eval") {
			t.Errorf("%s: CSP must not allow inline/eval: %s", path, csp)
		}
		if rec.Header().Get("X-Content-Type-Options") != "nosniff" || rec.Header().Get("X-Frame-Options") != "DENY" ||
			rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("%s: missing hardening headers: %v", path, rec.Header())
		}
	}
}

func TestIndexIsCSPCompatible(t *testing.T) {
	body := get(t, "/ui/").Body.String()
	for _, bad := range []string{"<script>", "onclick=", "onload=", "style=", "javascript:", "http://", "https://"} {
		if strings.Contains(body, bad) {
			t.Errorf("index.html contains CSP-incompatible or external content %q", bad)
		}
	}
	if !strings.Contains(body, `src="app.js"`) || !strings.Contains(body, `href="style.css"`) {
		t.Error("index.html should reference local assets only")
	}
}

func TestNoSecretsOrExternalResourcesInAssets(t *testing.T) {
	for _, p := range []string{"/ui/app.js", "/ui/lib.js", "/ui/style.css"} {
		b := get(t, p).Body.String()
		if strings.Contains(b, "innerHTML") && !strings.Contains(b, "never") {
			t.Errorf("%s uses innerHTML", p)
		}
		if strings.Contains(b, "eval(") || strings.Contains(b, "document.write") {
			t.Errorf("%s uses eval/document.write", p)
		}
		for _, ext := range []string{"https://", "//cdn", "@import", "localStorage"} {
			if strings.Contains(b, ext) {
				t.Errorf("%s references %q (token must stay in sessionStorage; no external resources)", p, ext)
			}
		}
	}
}

func TestUnknownAssetIs404(t *testing.T) {
	if rec := get(t, "/ui/nope.js"); rec.Code != 404 {
		t.Fatalf("status %d", rec.Code)
	}
}
