package main

import (
	"net/http"
	"strings"
)

// DeploymentIDHeader is what Next.js sends on client navigations and prefetches when the build
// has a deploymentId; static asset URLs carry the same id as ?dpl=.
const DeploymentIDHeader = "X-Deployment-Id"

// deploymentID extracts the build a request belongs to: the header Next's
// client router sends, else the `?dpl=` query Next appends to every
// /_next/static asset URL (and next/image URLs). Values that can't be a
// label value are ignored, i.e. treated as unpinned.
func deploymentID(r *http.Request) string {
	id := r.Header.Get(DeploymentIDHeader)
	if id == "" && strings.Contains(r.URL.RawQuery, "dpl=") {
		id = r.URL.Query().Get("dpl")
	}
	if !validID(id) {
		return ""
	}
	return id
}

// validID matches a Kubernetes label value (what the id is stored as).
func validID(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		alnum := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if !alnum && !((c == '-' || c == '_' || c == '.') && i > 0 && i < len(s)-1) {
			return false
		}
	}
	return true
}
