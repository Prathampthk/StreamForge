# Currency validation snapshot

Source: https://www.six-group.com/dam/download/financial-information/data-center/iso-currrency/lists/list-one.xml

Fetched 2026-11-06; XML publication attribute: {'Pblshd': '2026-09-17'}. SHA256 of source XML: 33139b438657d1cee116ba737807ea71d19d6de4b90f799a09c56f0cc6a1b0ff. `internal/domain/currencies.txt` contains sorted unique Ccy codes, excluding XXX (no currency) and XTS (testing). This is a reviewed static snapshot; no network call on ingestion. Includes official funds/metal codes. Amounts are integer minor units according to the selected code; no exchange conversion. Update the snapshot through a reviewed contract change. Runtime validation rejects unknown codes; JSON Schema limits currency syntax only.
