package provider

import "regexp"

var sheetID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)

func ValidSheetID(value string) bool { return sheetID.MatchString(value) }
