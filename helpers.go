package main

import "strings"

func camelCaseString(s string) string {
	if s == "" {
		return s
	}

	splitted := strings.Split(s, "_")

	if len(splitted) == 1 {
		return caser.String(s)
	}

	var cc string
	for _, part := range splitted {
		cc += caser.String(strings.ToLower(part))
	}
	return cc
}
