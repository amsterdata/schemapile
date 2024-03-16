# SchemaPile-Perm: Permissively Licensed Subset of SchemaPile

## SchemaPile-Perm
We provide the fully-parsed and ready-to-use subset of SchemaPile
as JSON-file, containing all schemas and data extracted from [permissively 
licensed](data/permissive_licenses.json) SchemaPile SQL files:

[data/schemapile-perm.json.gz](https://drive.google.com/file/d/1awg39JpTZJ9ty3mAnDUNJfK0h194bnJV/view?usp=sharing)

Note: PII values are imputed by `<PII-Type>`.
The following types were imputed:
`<LOCATION>`,`<PERSON>`,`<EMAIL_ADDRESS>`,`<PHONE_NUMBER>`,`<UK_NHS>`,
`<AU_TFN>`,`<AU_MEDICARE>`,`<AU_ABN>`,`<IP_ADDRESS>`,`<AU_ACN>`,
`<MEDICAL_LICENSE>`,`<CREDIT_CARD>`,`<US_SSN>`,`<SG_NRIC_FIN>`,
`<IBAN_CODE>`,`<CRYPTO>`

## SchemaPile-Perm SQL Files
This `tar.gz` file contains the raw SQL files from SchemaPile that are
permissively licensed:

[data/schemapile-perm-files.tar.gz](https://drive.google.com/file/d/1vyQEtUTsPKEq2e1bB1lYgJCDDiB39ptY/view?usp=sharing)

Note: License information per file can be found [here](sqlfiles-and-licenses.md).
The list of permissive licenses was adopted from [BigCode Project/The Stack](https://huggingface.co/datasets/bigcode/the-stack#licensing-information)