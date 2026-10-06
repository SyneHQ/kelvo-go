// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package saas

import "strings"

type stripeEntity struct {
	endpoint string
	fields   map[string]bool
}

var stripeEntities = func() map[string]stripeEntity {
	values := map[string][2]string{
		"customers":                     {"/v1/customers", "id email name phone description created balance currency default_source delinquent invoice_prefix livemode metadata next_invoice_sequence preferred_locales shipping tax_exempt test_clock"},
		"charges":                       {"/v1/charges", "id amount amount_captured amount_refunded application application_fee application_fee_amount balance_transaction billing_details calculated_statement_descriptor captured created currency customer description disputed failure_code failure_message fraud_details invoice livemode metadata outcome paid payment_intent payment_method payment_method_details receipt_email receipt_number receipt_url refunded refunds review shipping source_transfer statement_descriptor statement_descriptor_suffix status transfer_data transfer_group"},
		"payment_intents":               {"/v1/payment_intents", "id amount amount_capturable amount_details amount_received application application_fee_amount automatic_payment_methods canceled_at cancellation_reason capture_method client_secret confirmation_method created currency customer description invoice last_payment_error latest_charge livemode metadata next_action on_behalf_of payment_method payment_method_options payment_method_types processing receipt_email review setup_future_usage shipping statement_descriptor statement_descriptor_suffix status transfer_data transfer_group"},
		"subscriptions":                 {"/v1/subscriptions", "id application application_fee_percent automatic_tax billing_cycle_anchor billing_thresholds cancel_at cancel_at_period_end canceled_at collection_method created currency current_period_end current_period_start customer days_until_due default_payment_method default_source default_tax_rates description discount ended_at items latest_invoice livemode metadata next_pending_invoice_item_invoice pause_collection payment_settings pending_invoice_item_interval pending_setup_intent pending_update schedule start_date status test_clock transfer_data trial_end trial_start"},
		"invoices":                      {"/v1/invoices", "id account_country account_name account_tax_ids amount_due amount_paid amount_remaining application application_fee_amount attempt_count attempted auto_advance automatic_tax billing_reason charge collection_method created currency custom_fields customer customer_address customer_email customer_name customer_phone customer_shipping customer_tax_exempt customer_tax_ids default_payment_method default_source default_tax_rates description discount discounts due_date ending_balance footer hosted_invoice_url invoice_pdf last_finalization_error latest_revision lines livemode metadata next_payment_attempt number on_behalf_of paid paid_out_of_band payment_intent payment_settings period_end period_start post_payment_credit_notes_amount pre_payment_credit_notes_amount quote receipt_number rendering_options starting_balance statement_descriptor status status_transitions subscription subtotal tax test_clock total total_discount_amounts total_tax_amounts transfer_data webhooks_delivered_at"},
		"products":                      {"/v1/products", "id active attributes caption created deactivate_on description images livemode metadata name package_dimensions shippable statement_descriptor tax_code type unit_label updated url"},
		"prices":                        {"/v1/prices", "id active billing_scheme created currency custom_unit_amount livemode lookup_key metadata nickname product recurring tax_behavior tiers tiers_mode transform_quantity type unit_amount unit_amount_decimal"},
		"payment_methods":               {"/v1/payment_methods", "id billing_details card created customer livemode metadata type us_bank_account"},
		"refunds":                       {"/v1/refunds", "id amount charge created currency metadata payment_intent reason receipt_number source_transfer_reversal status transfer_reversal"},
		"balance_transactions":          {"/v1/balance_transactions", "id amount available_on created currency description exchange_rate fee fee_details net reporting_category source status type"},
		"subscription_items":            {"/v1/subscription_items", "id subscription price quantity created metadata billing_thresholds tax_rates"},
		"coupons":                       {"/v1/coupons", "id amount_off created currency duration duration_in_months livemode max_redemptions metadata name percent_off redeem_by times_redeemed valid"},
		"promotion_codes":               {"/v1/promotion_codes", "id active code coupon created customer expires_at livemode max_redemptions metadata restrictions times_redeemed"},
		"usage_records":                 {"/v1/subscription_items/{subscription_item_id}/usage_records", "id livemode quantity subscription_item timestamp"},
		"tax_rates":                     {"/v1/tax_rates", "id active country created description display_name inclusive jurisdiction livemode metadata percentage state tax_type"},
		"credit_notes":                  {"/v1/credit_notes", "id amount created currency customer customer_balance_transaction discount_amount discount_amounts invoice lines livemode memo metadata number out_of_band_amount pdf reason refund status subtotal tax_amounts total type voided_at"},
		"invoice_items":                 {"/v1/invoiceitems", "id amount currency customer date description discountable discounts invoice livemode metadata period price proration quantity subscription tax_rates unit_amount unit_amount_decimal"},
		"billing_portal_sessions":       {"/v1/billing_portal/sessions", "id configuration created customer livemode locale on_behalf_of return_url url"},
		"billing_portal_configurations": {"/v1/billing_portal/configurations", "id active application business_profile created default_return_url features is_default livemode metadata updated"},
		"payment_links":                 {"/v1/payment_links", "id active after_completion allow_promotion_codes application application_fee_amount application_fee_percent automatic_tax billing_address_collection created currency custom_fields custom_text customer_creation invoice_creation line_items livemode metadata on_behalf_of payment_intent_data payment_method_collection payment_method_types phone_number_collection restrictions shipping_address_collection shipping_options submit_type subscription_data tax_id_collection transfer_data url"},
		"subscription_schedules":        {"/v1/subscription_schedules", "id application canceled_at completed_at created current_phase customer default_settings end_behavior livemode metadata phases released_at released_subscription status subscription test_clock"},
		"test_clocks":                   {"/v1/test_helpers/test_clocks", "id created deletes_after frozen_time livemode name status"},
	}
	result := map[string]stripeEntity{}
	for name, value := range values {
		fields := map[string]bool{}
		for _, field := range strings.Fields(value[1]) {
			fields[field] = true
		}
		result[name] = stripeEntity{endpoint: value[0], fields: fields}
	}
	return result
}()
