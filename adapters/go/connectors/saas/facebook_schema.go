// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package saas

var facebookDimensions = map[string]string{
	"date":               "date_start",
	"account_id":         "account_id",
	"campaign_id":        "campaign_id",
	"campaign_name":      "campaign_name",
	"adset_id":           "adset_id",
	"adset_name":         "adset_name",
	"ad_id":              "ad_id",
	"ad_name":            "ad_name",
	"country":            "country",
	"region":             "region",
	"impression_device":  "impression_device",
	"device_platform":    "device_platform",
	"publisher_platform": "publisher_platform",
	"platform_position":  "platform_position",
	"age":                "age",
	"gender":             "gender",
}
var facebookMetrics = map[string]bool{
	"impressions":               true,
	"clicks":                    true,
	"spend":                     true,
	"reach":                     true,
	"frequency":                 true,
	"ctr":                       true,
	"cpc":                       true,
	"cpm":                       true,
	"cpp":                       true,
	"inline_link_clicks":        true,
	"inline_link_click_ctr":     true,
	"unique_clicks":             true,
	"unique_inline_link_clicks": true,
	"actions":                   true, // required when any action metric is requested
	"action_values":             true,
}
